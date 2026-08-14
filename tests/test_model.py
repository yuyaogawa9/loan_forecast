"""Multi-state modelling: state space, Arrow ingestion, matrices, projection."""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from loan_etl.features import load_registry, select
from loan_model.dataset import (
    MISSING_CODE,
    CategoricalEncoder,
    DatasetError,
    build_dataset,
    to_arrow_features,
)
from loan_model.project import (
    ProjectionError,
    Scenario,
    assert_markov_safe,
    project_loan,
)
from loan_model.registry import TransitionModel
from loan_model.states import StateError, load_transition_config
from loan_model.transition import (
    StateSpace,
    TransitionError,
    build_matrix,
    validate_row_stochastic,
)


@pytest.fixture(scope="module")
def cfg():
    return load_transition_config()


# --- state space -----------------------------------------------------------


def test_every_from_state_includes_the_fallback(cfg):
    """Rare destinations fold into OTHER_TERMINAL; without it in the label set
    that probability mass would be silently deleted and rows would not sum to 1."""
    for fs in cfg.from_states:
        assert cfg.fallback_state in cfg.destinations(fs), fs


def test_no_state_is_both_transient_and_absorbing(cfg):
    assert not (set(cfg.transient_states) & set(cfg.absorbing_states))


def test_every_transient_state_is_reachable(cfg):
    reachable = {d for fs in cfg.from_states for d in cfg.destinations(fs)}
    assert set(cfg.transient_states) <= reachable


def test_from_state_and_destination_alphabets_align(cfg):
    """Every transient state must route to a real model, and every model must be
    reachable as a state.

    v1 violated this: from-states bucketed at 6 months while EVENT collapses at
    3, so DLQ_90_PLUS always used the from-03 model -- which had no CHARGEOFF or
    REO destination. Default mass cycled forever and projected losses were
    exactly zero, with no error raised anywhere.
    """
    routed = {cfg.model_key_for_state(s) for s in cfg.transient_states}
    assert routed == set(cfg.from_states)
    for state in cfg.transient_states:
        assert cfg.model_key_for_state(state) in cfg.models, state


def test_deep_delinquency_state_can_reach_charge_off(cfg):
    """The whole point of aligning the alphabets."""
    key = cfg.model_key_for_state("DLQ_90_PLUS")
    dests = set(cfg.destinations(key))
    assert {"CHARGEOFF", "REO"} <= dests, dests


def test_label_maps_are_contiguous_from_zero(cfg):
    """LightGBM requires labels 0..K-1; a gap silently mislabels every class."""
    for fs in cfg.from_states:
        m = cfg.label_map(fs)
        assert sorted(m.values()) == list(range(len(m))), fs


@pytest.mark.parametrize(
    "status,expected",
    [("00", "00"), ("01", "01"), ("02", "02"), ("03", "03_PLUS"),
     ("05", "03_PLUS"), ("11", "03_PLUS"), ("RA", "RA"), ("XX", None), (None, None)],
)
def test_from_state_bucketing(cfg, status, expected):
    assert cfg.bucket_from_state(status) == expected


def test_from_state_expr_matches_scalar_version(cfg):
    vals = ["00", "01", "02", "03", "09", "RA", None]
    got = (
        pl.DataFrame({"PRIOR_DLQ_STATUS": vals})
        .select(cfg.from_state_expr())["FROM_STATE"]
        .to_list()
    )
    assert got == [cfg.bucket_from_state(v) for v in vals]


def test_collapse_conserves_every_row(cfg):
    """Out-of-set destinations must map to the fallback, never vanish."""
    events = ["CURRENT", "PREPAID", "CHARGEOFF", "REO", "CREDIT_EVENT_OTHER"]
    out = (
        pl.DataFrame({"EVENT": events})
        .select(cfg.destination_expr("00"))["DESTINATION"]
        .to_list()
    )
    assert len(out) == len(events)
    assert out[2:] == [cfg.fallback_state] * 3   # not in from-00's destination set
    assert set(out) <= set(cfg.destinations("00"))


# --- categorical encoding --------------------------------------------------


def test_encoder_is_stable_across_frames():
    """polars categorical codes are frame-local; the encoder must not be.

    Encoding train and validation separately with to_physical() would assign
    different integers to the same level and silently corrupt the model.
    """
    train = pl.DataFrame({"s": ["CA", "TX", "NY", "CA"]})
    valid = pl.DataFrame({"s": ["NY", "CA"]})          # different order and subset
    enc = CategoricalEncoder.fit(train, ["s"])

    a = enc.transform(train)["s"].to_list()
    b = enc.transform(valid)["s"].to_list()
    assert dict(zip(train["s"], a))["NY"] == dict(zip(valid["s"], b))["NY"]

    naive_train = train.with_columns(pl.col("s").cast(pl.Categorical).to_physical())["s"].to_list()
    naive_valid = valid.with_columns(pl.col("s").cast(pl.Categorical).to_physical())["s"].to_list()
    assert dict(zip(train["s"], naive_train))["NY"] != dict(zip(valid["s"], naive_valid))["NY"], (
        "if this ever passes, polars changed and the encoder may be redundant"
    )


def test_unseen_category_becomes_missing_not_a_collision():
    enc = CategoricalEncoder.fit(pl.DataFrame({"s": ["CA", "TX"]}), ["s"])
    got = enc.transform(pl.DataFrame({"s": ["CA", "ZZ"]}))["s"].to_list()
    assert got[1] == MISSING_CODE
    assert got[0] != MISSING_CODE


def test_encoder_round_trips_through_json():
    enc = CategoricalEncoder.fit(pl.DataFrame({"s": ["CA", "TX"]}), ["s"])
    assert CategoricalEncoder.from_dict(enc.to_dict()).mapping == enc.mapping


# --- Arrow ingestion -------------------------------------------------------


def _frame(n=200, seed=0):
    rng = np.random.default_rng(seed)
    return pl.DataFrame(
        {
            "x1": rng.normal(size=n),
            "x2": rng.normal(size=n),
            "state": rng.choice(["CA", "TX", "NY"], n),
            "LABEL": rng.integers(0, 3, n),
            "SAMPLE_WEIGHT": np.ones(n),
        }
    )


def test_arrow_features_are_numeric_only():
    df = _frame()
    enc = CategoricalEncoder.fit(df, ["state"])
    t = to_arrow_features(df, ["x1", "x2", "state"], enc)
    kinds = {f.name: str(f.type) for f in t.schema}
    assert kinds["x1"] == "float" and kinds["x2"] == "float"
    assert kinds["state"] == "int32"


def test_dictionary_columns_are_rejected_loudly():
    """LightGBM's own error for this is opaque; fail with an actionable one."""
    df = _frame().with_columns(pl.col("state").cast(pl.Categorical))
    with pytest.raises(DatasetError, match="dictionary-encoded"):
        to_arrow_features(df, ["state"], encoder=None)


def test_dataset_builds_without_pandas(monkeypatch):
    """The whole point of the Arrow path: pandas must never be constructed."""
    import pandas as pd

    def explode(*a, **k):
        raise AssertionError("pandas.DataFrame was constructed in the Arrow path")

    monkeypatch.setattr(pd, "DataFrame", explode)
    df = _frame()
    enc = CategoricalEncoder.fit(df, ["state"])
    ds = build_dataset(
        df, ["x1", "x2", "state"], "LABEL", encoder=enc, weight_column="SAMPLE_WEIGHT"
    )
    ds.construct()
    assert ds.num_data() == df.height


# --- transition matrices ---------------------------------------------------


def _toy_model(from_state, destinations, features=("x1", "x2")):
    import lightgbm as lgb

    rng = np.random.default_rng(1)
    n, k = 500, len(destinations)
    df = pl.DataFrame({f: rng.normal(size=n) for f in features}).with_columns(
        pl.Series("LABEL", rng.integers(0, k, n))
    )
    enc = CategoricalEncoder(mapping={})
    ds = build_dataset(df, list(features), "LABEL", encoder=enc)
    booster = lgb.train(
        {"objective": "multiclass", "num_class": k, "verbose": -1, "num_leaves": 4},
        ds,
        num_boost_round=3,
    )
    return TransitionModel(
        from_state=from_state,
        booster=booster,
        features=list(features),
        label_map={d: i for i, d in enumerate(destinations)},
        encoder=enc,
        markov_safe=True,
        metrics={},
        meta={},
    )


@pytest.fixture(scope="module")
def toy(cfg):
    models = {fs: _toy_model(fs, cfg.destinations(fs)) for fs in cfg.from_states}
    covariates = {
        fs: pl.DataFrame({"x1": [0.1], "x2": [-0.2]}) for fs in cfg.from_states
    }
    return models, covariates


def test_matrix_rows_sum_to_one(cfg, toy):
    models, cov = toy
    space = StateSpace.from_config(cfg)
    P = build_matrix(models, cov, space, cfg)
    assert np.allclose(P.sum(axis=1), 1.0, atol=1e-9)


def test_absorbing_rows_are_identity(cfg, toy):
    models, cov = toy
    space = StateSpace.from_config(cfg)
    P = build_matrix(models, cov, space, cfg)
    for s in cfg.absorbing_states:
        i = space.index(s)
        assert P[i, i] == pytest.approx(1.0)
        assert P[i].sum() == pytest.approx(1.0)


def test_row_stochastic_check_actually_fires(cfg):
    space = StateSpace.from_config(cfg)
    P = np.zeros((space.size, space.size))
    P[0, 0] = 0.5   # row does not sum to 1
    with pytest.raises(TransitionError, match="do not sum to 1"):
        validate_row_stochastic(P, space)


# --- projection ------------------------------------------------------------


def test_projection_rejects_path_dependent_models(cfg, toy):
    models, _ = toy
    unsafe = dict(models)
    fs = next(iter(unsafe))
    unsafe[fs] = TransitionModel(**{**unsafe[fs].__dict__, "markov_safe": False})
    with pytest.raises(ProjectionError, match="path-dependent"):
        assert_markov_safe(unsafe)


def test_path_dependent_features_are_excluded_by_markov_filter():
    reg = load_registry()
    assert reg.path_dependent_features, "columns.yaml declares none"
    cols = list(reg.path_dependent_features) + [
        "LOAN_AGE", "CREDIT_SCORE", "EVENT", "LOAN_SEQUENCE_NUMBER",
        "MONTHLY_REPORTING_PERIOD", "vintage_year",
    ]
    sel = select(cols, "competing_risks", markov_safe_only=True)
    assert not (set(sel.features) & reg.path_dependent_features)


def test_projection_distribution_stays_a_distribution(cfg, toy):
    """The invariant that matters: mass is conserved at every step."""
    models, _ = toy
    features = models["00"].features
    scenario = Scenario(pl.DataFrame({"MONTHLY_REPORTING_PERIOD": ["200701"]}))
    loan = {
        "ORIGINAL_UPB": 200_000.0,
        "ORIGINAL_INTEREST_RATE": 6.0,
        "ORIGINAL_LOAN_TERM": 360.0,
        "MONTHLY_REPORTING_PERIOD": "200701",
        **{f: 0.0 for f in features},
    }
    res = project_loan(models, cfg, loan, scenario, horizon=12, severity=0.4)
    assert res.distribution.shape == (13, StateSpace.from_config(cfg).size)
    assert np.allclose(res.distribution.sum(axis=1), 1.0, atol=1e-9)
    assert res.expected_loss >= 0.0


def test_projection_balance_amortises_downward(cfg, toy):
    models, _ = toy
    features = models["00"].features
    scenario = Scenario(pl.DataFrame({"MONTHLY_REPORTING_PERIOD": ["200701"]}))
    loan = {
        "ORIGINAL_UPB": 200_000.0,
        "ORIGINAL_INTEREST_RATE": 6.0,
        "ORIGINAL_LOAN_TERM": 360.0,
        **{f: 0.0 for f in features},
    }
    res = project_loan(models, cfg, loan, scenario, horizon=24)
    assert res.balance[0] == pytest.approx(200_000.0, abs=1.0)
    assert np.all(np.diff(res.balance) <= 1e-6)


def test_period_advances_across_year_boundary(cfg, toy):
    models, _ = toy
    scenario = Scenario(pl.DataFrame({"MONTHLY_REPORTING_PERIOD": ["200711"]}))
    loan = {
        "ORIGINAL_UPB": 100_000.0, "ORIGINAL_INTEREST_RATE": 5.0,
        "ORIGINAL_LOAN_TERM": 360.0,
        **{f: 0.0 for f in models["00"].features},
    }
    res = project_loan(models, cfg, loan, scenario, horizon=3, start_period="200711")
    assert res.periods == ["200711", "200712", "200801", "200802"]


def test_misaligned_alphabet_is_rejected_at_load(tmp_path):
    """Reconstruct the v1 bug and confirm it now fails loudly.

    Bucketing from-states at 6 months while EVENT collapses at 3 leaves
    DLQ_90_PLUS routed to a model that does not exist. Previously silent.
    """
    import yaml
    from loan_model.states import CONFIG_PATH, load_transition_config as load

    spec = yaml.safe_load(CONFIG_PATH.read_text())
    spec["from_state"]["deep_delinquency_min_months"] = 6
    spec["from_state"]["deep_delinquency_label"] = "06_PLUS"   # no such model
    bad = tmp_path / "transitions.yaml"
    bad.write_text(yaml.safe_dump(spec))

    with pytest.raises(StateError, match="no model"):
        load(bad)
