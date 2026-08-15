"""Conditional amount models: calibration, monotonicity, and the balance path.

Quantile calibration is the gate. A quantile model can track the conditional
median well and still be a useless distribution, and only coverage establishes
that the SPREAD is right -- which is the entire reason to fit quantiles instead
of a mean.
"""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from loan_etl.settings import SCHEMA_DIR, Settings
from loan_model.amounts import (
    DEFAULT_QUANTILES,
    AmountError,
    BinaryHazardModel,
    QuantileAmountModel,
    load_amount_model,
    quantile_calibration,
    save_amount_model,
)
from loan_model.dataset import CategoricalEncoder, build_dataset
from loan_model.simulate import principal_factor, simulate_chunk, uniforms
from loan_model.states import load_transition_config
from loan_model.train_amounts import SEVERITY_FORBIDDEN, OUTCOME_FEATURE

FAST = {"num_boost_round": 60, "early_stopping_rounds": 20}


def _heteroscedastic(n: int = 6_000, seed: int = 0) -> pl.DataFrame:
    """Spread that grows with x, so a mean model provably cannot fit it."""
    rng = np.random.default_rng(seed)
    x = rng.uniform(0, 1, n)
    group = rng.choice(["A", "B"], n)
    noise = rng.normal(0, 1, n) * (0.05 + 0.5 * x)
    y = 0.4 + 0.3 * x + np.where(group == "A", 0.0, 0.25) + noise
    return pl.DataFrame({"x": x, "group": group, "y": y})


@pytest.fixture(scope="module")
def fitted():
    df = _heteroscedastic()
    train, valid = df.head(4_000), df.tail(2_000)
    model = QuantileAmountModel.fit(
        train, "y", ["x", "group"], valid=valid, categorical_hint=("group",), **FAST
    )
    return model, valid


# --- the gate --------------------------------------------------------------


def test_quantile_coverage_matches_the_nominal_level(fitted):
    """Share of actuals below the q-th predicted quantile must be ~q."""
    model, valid = fitted
    calibration = quantile_calibration(model, valid)
    worst = float(calibration["error"].abs().max())
    assert worst < 0.05, calibration.to_dicts()


def test_the_spread_widens_with_the_covariate(fitted):
    """What a conditional-mean model cannot do, and the reason for quantiles."""
    model, _ = fitted
    narrow = model.predict_quantiles(pl.DataFrame({"x": [0.05], "group": ["A"]}))[0]
    wide = model.predict_quantiles(pl.DataFrame({"x": [0.95], "group": ["A"]}))[0]
    assert (wide[-1] - wide[0]) > 2 * (narrow[-1] - narrow[0])


def test_sampling_reproduces_the_fitted_distribution(fitted):
    """Inverting the curve must return the distribution it describes."""
    model, valid = fitted
    draws = model.sample(valid, uniforms(np.arange(valid.height, dtype=np.uint64), 1, 3))
    actual = valid["y"].to_numpy()
    for q in (0.25, 0.5, 0.75):
        assert abs(np.quantile(draws, q) - np.quantile(actual, q)) < 0.08, q


# --- monotonicity ----------------------------------------------------------


def test_predicted_quantiles_never_cross(fitted):
    """Independently fitted quantiles do cross; unrepaired that yields a
    non-monotone inverse CDF and therefore invalid draws."""
    model, valid = fitted
    curve = model.predict_quantiles(valid)
    assert (np.diff(curve, axis=1) >= -1e-12).all()


def test_crossing_is_repaired_rather_than_merely_absent(fitted):
    """Proves the repair runs, instead of relying on the fixture to cross.

    A well-behaved synthetic fit happens not to cross; real severity data crosses
    on 29% of rows. Reversing the boosters forces maximal crossing, so if the
    sort were removed this fails deterministically.
    """
    model, valid = fitted
    from loan_model.dataset import to_arrow_features

    scrambled = QuantileAmountModel(
        target=model.target,
        quantiles=model.quantiles,
        boosters=list(reversed(model.boosters)),
        features=model.features,
        encoder=model.encoder,
    )
    table = to_arrow_features(valid, model.features, model.encoder)
    raw = np.column_stack([b.predict(table) for b in scrambled.boosters])
    assert (np.diff(raw, axis=1) < -1e-12).any(), "reversal should have crossed"

    repaired = scrambled.predict_quantiles(valid)
    assert (np.diff(repaired, axis=1) >= -1e-12).all()
    assert np.allclose(repaired, np.sort(raw, axis=1))


def test_clip_bounds_are_respected():
    df = _heteroscedastic()
    model = QuantileAmountModel.fit(
        df.head(3_000), "y", ["x"], valid=df.tail(1_000), clip=(0.0, 1.0), **FAST
    )
    curve = model.predict_quantiles(df.tail(1_000))
    assert curve.min() >= 0.0 and curve.max() <= 1.0
    draws = model.sample(df.tail(1_000), np.full(1_000, 0.999))
    assert draws.max() <= 1.0


def test_draws_saturate_rather_than_extrapolating(fitted):
    """A linear extrapolation off the 99th percentile of a fat-tailed loss
    distribution produces values with no support in the data."""
    model, valid = fitted
    curve = model.predict_quantiles(valid)
    assert model.sample(valid, np.zeros(valid.height)).max() <= curve[:, 0].max() + 1e-9
    assert model.sample(valid, np.full(valid.height, 1.0 - 1e-12)).min() >= curve[:, -1].min() - 1e-9


# --- the regression that made severity look like a fit ----------------------


def test_regression_targets_are_not_truncated_to_integers():
    """`build_dataset` casts labels to Int32 for the transition multinomials.

    Left at that default a severity of 0.453 becomes 0, and every quantile is
    then fitted against a variable that is zero almost everywhere -- which looks
    like a model, not like an error. It made predicted median coverage come out
    at 4.5% against a nominal 50%.
    """
    df = pl.DataFrame({"x": [0.1, 0.9, 0.5, 0.3], "y": [0.45, 0.62, 0.51, 0.30]})
    integral = build_dataset(df, ["x"], "y", encoder=CategoricalEncoder(mapping={}))
    integral.construct()
    assert set(integral.get_label()) == {0.0}, "expected the truncating default"

    floating = build_dataset(
        df, ["x"], "y", encoder=CategoricalEncoder(mapping={}), label_dtype=pl.Float32
    )
    floating.construct()
    assert np.allclose(sorted(floating.get_label()), [0.30, 0.45, 0.51, 0.62], atol=1e-6)


# --- conditioning on the outcome -------------------------------------------


def test_severity_conditions_on_which_outcome_occurred():
    """Realised severity is 53.8% for REO disposition against 37.0% for
    charge-off. Pooling them averages away the strongest available signal."""
    rng = np.random.default_rng(3)
    n = 4_000
    outcome = rng.choice(["REO_DISPOSITION", "CHARGEOFF"], n)
    y = np.where(outcome == "REO_DISPOSITION", 0.54, 0.37) + rng.normal(0, 0.08, n)
    df = pl.DataFrame({OUTCOME_FEATURE: outcome, "x": rng.normal(size=n), "y": y})

    model = QuantileAmountModel.fit(
        df.head(3_000), "y", [OUTCOME_FEATURE, "x"], valid=df.tail(1_000),
        categorical_hint=(OUTCOME_FEATURE,), **FAST
    )
    reo = model.predict_median(
        pl.DataFrame({OUTCOME_FEATURE: ["REO_DISPOSITION"], "x": [0.0]})
    )[0]
    charged = model.predict_median(
        pl.DataFrame({OUTCOME_FEATURE: ["CHARGEOFF"], "x": [0.0]})
    )[0]
    assert reo - charged > 0.10, (reo, charged)


def test_loss_identity_columns_are_barred_from_the_severity_model():
    """Conditioning on the OUTCOME is the point; conditioning on the realised
    loss would be circular."""
    for column in ("ACTUAL_LOSS_CALCULATION", "TOTAL_RECOVERIES", "ZERO_BALANCE_REMOVAL"):
        assert column in SEVERITY_FORBIDDEN


# --- hazard ----------------------------------------------------------------


def test_binary_hazard_recovers_the_underlying_rate():
    rng = np.random.default_rng(5)
    n = 8_000
    x = rng.uniform(0, 1, n)
    p = 0.1 + 0.6 * x
    df = pl.DataFrame({"x": x, "hit": (rng.uniform(0, 1, n) < p).astype(np.int8)})
    model = BinaryHazardModel.fit(
        df.head(6_000), "hit", ["x"], valid=df.tail(2_000), num_boost_round=80
    )
    predicted = model.predict(df.tail(2_000))
    assert abs(predicted.mean() - df.tail(2_000)["hit"].mean()) < 0.03
    low = model.predict(pl.DataFrame({"x": [0.05]}))[0]
    high = model.predict(pl.DataFrame({"x": [0.95]}))[0]
    assert high - low > 0.3


def test_hazard_occurrence_respects_the_uniform_stream():
    rng = np.random.default_rng(6)
    df = pl.DataFrame({"x": rng.uniform(0, 1, 2_000), "hit": rng.integers(0, 2, 2_000)})
    model = BinaryHazardModel.fit(df, "hit", ["x"], num_boost_round=20)
    assert model.occurs(df, np.zeros(df.height)).all()
    assert not model.occurs(df, np.ones(df.height)).any()


# --- persistence -----------------------------------------------------------


def test_amount_models_round_trip(tmp_path, fitted):
    model, valid = fitted
    settings = Settings(
        data_root=tmp_path / "lake", fred_api_key="k", schema_version=47, schema_dir=SCHEMA_DIR
    )
    settings.ensure_dirs()
    save_amount_model(settings, "test", model)
    restored = load_amount_model(settings, "test", "y")

    assert restored.quantiles == model.quantiles
    assert restored.features == model.features
    assert np.allclose(restored.predict_quantiles(valid), model.predict_quantiles(valid))


def test_missing_features_are_refused(fitted):
    model, valid = fitted
    with pytest.raises(AmountError, match="missing features"):
        model.predict_quantiles(valid.drop("x"))


# --- balance as path state -------------------------------------------------


def test_delinquent_loans_stop_paying_down_principal():
    """Observed medians: 0.202% of balance paid when current, 0.088% at two
    months, exactly 0.000% at 90+. That is what makes exposure at default land
    2.4% above the origination schedule."""
    table = {"0": 1.0, "1": 0.75, "2": 0.44, "3_plus": 0.0}
    factors = principal_factor(np.array([0, 1, 2, 3, 7, -1]), table)
    assert factors.tolist() == [1.0, 0.75, 0.44, 0.0, 0.0, 0.0]


def test_curtailment_reduces_the_simulated_balance():
    """Curtailment is 32.7% of all principal reduction. A simulation that
    ignores it holds balances too high for the whole horizon."""
    cfg = load_transition_config()
    from .test_simulate import _loans, _toy_model

    models = {fs: _toy_model(fs, cfg.destinations(fs)) for fs in cfg.from_states}
    loans = _loans(400)

    always = _ConstantHazard(1.0)
    never = _ConstantHazard(0.0)
    amount = _ConstantFraction(0.02)

    with_curtail, _ = simulate_chunk(
        models, cfg, loans, None, horizon=6, seed=1,
        prepay_hazard=always, curtailment=amount,
    )
    without, _ = simulate_chunk(models, cfg, loans, None, horizon=6, seed=1,
                                prepay_hazard=never, curtailment=amount)

    def final_upb(frame):
        last = frame.filter(pl.col("PERIOD") == frame["PERIOD"].max())
        return float(last["UPB"].sum())

    assert final_upb(with_curtail) < final_upb(without) * 0.95
    assert float(with_curtail["CURTAILMENT"].sum()) > 0
    assert float(without["CURTAILMENT"].sum()) == 0


class _ConstantHazard:
    features = ["x1"]

    def __init__(self, rate: float):
        self.rate = rate

    def occurs(self, frame, uniforms):
        return np.full(frame.height, self.rate > 0.5)


class _ConstantFraction:
    features = ["x1"]

    def __init__(self, value: float):
        self.value = value

    def sample(self, frame, uniforms):
        return np.full(frame.height, self.value)


def test_severity_receives_the_outcome_that_occurred():
    """The severity model conditions on WHICH terminal event happened, so the
    destination has to travel with the slice under the trained column name."""
    cfg = load_transition_config()
    from .test_simulate import _loans, _toy_model

    models = {fs: _toy_model(fs, cfg.destinations(fs)) for fs in cfg.from_states}
    seen: list[list[str]] = []

    class Recorder:
        def draw(self, frame, uniforms):
            seen.append(frame[OUTCOME_FEATURE].to_list())
            return np.full(frame.height, 0.5)

    simulate_chunk(models, cfg, _loans(2_000), None, horizon=12, seed=2, severity=Recorder())
    assert seen, "severity was never asked for a draw"
    events = {e for batch in seen for e in batch}
    assert events <= {"CHARGEOFF", "REO_DISPOSITION", "CREDIT_EVENT_OTHER"}


def test_default_quantile_grid_spans_both_tails():
    assert DEFAULT_QUANTILES[0] <= 0.01 and DEFAULT_QUANTILES[-1] >= 0.99
    assert 0.5 in DEFAULT_QUANTILES
