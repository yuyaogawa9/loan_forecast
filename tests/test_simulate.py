"""Monte Carlo engine: sampler correctness, batching, determinism, severity.

The batching test is the one that guards the design. Everything else about this
module is a normal correctness check; if predict-call count starts scaling with
the number of paths, simulation silently becomes unusable at portfolio size
rather than wrong, which is harder to notice.
"""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from loan_model.dataset import CategoricalEncoder, build_dataset
from loan_model.lgd import EmpiricalSeverity, SeverityError
from loan_model.registry import TransitionModel
from loan_model.scenario import MacroPanel, Scenario
from loan_model.simulate import (
    FlatSeverity,
    sample_destinations,
    simulate,
    simulate_chunk,
    uniforms,
)
from loan_model.states import load_transition_config
from loan_model.transition import StateSpace

FEATURES = ("x1", "x2")


@pytest.fixture(scope="module")
def cfg():
    return load_transition_config()


def _toy_model(from_state, destinations, features=FEATURES, seed=1):
    import lightgbm as lgb

    rng = np.random.default_rng(seed)
    n, k = 400, len(destinations)
    df = pl.DataFrame({f: rng.normal(size=n) for f in features}).with_columns(
        pl.Series("LABEL", rng.integers(0, k, n))
    )
    enc = CategoricalEncoder(mapping={})
    booster = lgb.train(
        {"objective": "multiclass", "num_class": k, "verbose": -1, "num_leaves": 4},
        build_dataset(df, list(features), "LABEL", encoder=enc),
        num_boost_round=3,
    )
    return TransitionModel(
        from_state=from_state,
        booster=booster,
        features=list(features),
        label_map={d: i for i, d in enumerate(destinations)},
        encoder=enc,
        markov_safe=False,
        metrics={},
        meta={},
    )


@pytest.fixture(scope="module")
def toy_models(cfg):
    return {fs: _toy_model(fs, cfg.destinations(fs)) for fs in cfg.from_states}


def _loans(n: int, seed: int = 0) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    return pl.DataFrame(
        {
            "LOAN_SEQUENCE_NUMBER": [f"F07Q1{i:07d}" for i in range(n)],
            "MONTHLY_REPORTING_PERIOD": ["200703"] * n,
            "x1": rng.normal(size=n),
            "x2": rng.normal(size=n),
            "ORIGINAL_UPB": np.full(n, 200_000.0),
            "ORIGINAL_INTEREST_RATE": np.full(n, 6.25),
            "ORIGINAL_LOAN_TERM": np.full(n, 360.0),
            "CURRENT_INTEREST_RATE": np.full(n, 6.25),
            "PROPERTY_STATE": ["CA"] * n,
            "MARK_TO_MARKET_LTV": np.full(n, 95.0),
        }
    )


# --- the sampler -----------------------------------------------------------


def test_inverse_cdf_sampling_reproduces_the_probabilities():
    """The estimator has to be unbiased or every projected rate is wrong."""
    proba = np.tile(np.array([0.6, 0.25, 0.1, 0.05]), (200_000, 1))
    draws = uniforms(np.arange(200_000, dtype=np.uint64), month=1, seed=7)
    picked = sample_destinations(proba, draws)

    observed = np.bincount(picked, minlength=4) / len(picked)
    assert np.allclose(observed, [0.6, 0.25, 0.1, 0.05], atol=0.005)


def test_sampling_never_indexes_past_the_destination_set():
    """Rows sum to 1 only up to float error; a draw above the last cumulative
    value would index off the end and pick a state the model cannot reach."""
    proba = np.full((5_000, 3), 1 / 3 - 1e-9)
    draws = np.linspace(0.0, 1.0 - 1e-12, 5_000)
    picked = sample_destinations(proba, draws)
    assert picked.min() >= 0 and picked.max() <= 2


def test_uniforms_are_uniform_and_stream_independent():
    keys = np.arange(50_000, dtype=np.uint64)
    u = uniforms(keys, month=3, seed=11)
    assert u.min() >= 0.0 and u.max() < 1.0
    assert abs(u.mean() - 0.5) < 0.01
    # The severity draw must not be correlated with the transition draw.
    other = uniforms(keys, month=3, seed=11, stream=1)
    assert abs(float(np.corrcoef(u, other)[0, 1])) < 0.02


def test_uniforms_depend_on_month_and_seed():
    keys = np.arange(1_000, dtype=np.uint64)
    base = uniforms(keys, month=1, seed=5)
    assert not np.array_equal(base, uniforms(keys, month=2, seed=5))
    assert not np.array_equal(base, uniforms(keys, month=1, seed=6))


# --- the property the design rests on --------------------------------------


def test_predict_calls_do_not_scale_with_path_count(cfg, toy_models, monkeypatch):
    """Cost must be horizon x from-states, NOT paths x horizon x from-states.

    A single-row predict costs ~3 ms of fixed overhead against 1.67 us/row when
    batched. If this regresses, a 50,000-path run goes from seconds to about a
    day -- so the call count is asserted directly rather than inferred from a
    timing measurement that would be flaky in CI.
    """
    import lightgbm as lgb

    calls = {"n": 0, "rows": 0}
    real_predict = lgb.Booster.predict

    def counting_predict(self, data, **kwargs):
        calls["n"] += 1
        calls["rows"] += data.num_rows
        return real_predict(self, data, **kwargs)

    monkeypatch.setattr(lgb.Booster, "predict", counting_predict)

    horizon, ceiling = 6, 6 * len(cfg.from_states)

    simulate_chunk(toy_models, cfg, _loans(10), None, horizon=horizon, seed=1)
    small, small_rows = calls["n"], calls["rows"]

    calls["n"] = calls["rows"] = 0
    simulate_chunk(toy_models, cfg, _loans(5_000), None, horizon=horizon, seed=1)
    large, large_rows = calls["n"], calls["rows"]

    # The hard bound: one call per (month, from-state) at most, whatever the
    # path count. Small runs come in UNDER it simply because rare from-states
    # stay empty, so the bound is the invariant -- not monotonicity.
    assert small <= ceiling and large <= ceiling, (small, large, ceiling)

    # 500x the paths must not mean anything like 500x the calls.
    assert large < small * 10, f"calls scaling with paths: {small} -> {large}"

    # ...while the ROWS scored do scale, which proves the paths are being
    # batched into those calls rather than quietly skipped.
    assert large_rows > 100 * small_rows


# --- chain correctness -----------------------------------------------------


def test_simulation_converges_to_the_matrix_projection(cfg):
    """The two engines must agree when duration plays no part.

    With features that never change, the transition matrix is constant, so the
    matrix recursion is the exact answer and Monte Carlo must converge to it.
    Any systematic gap here would mean the sampler, the state routing or the
    absorbing-state handling is biased -- and on real models that bias would be
    indistinguishable from a genuine credit signal.
    """
    from loan_model.project import project_loan

    models = {
        fs: _toy_model(fs, cfg.destinations(fs), seed=2) for fs in cfg.from_states
    }
    for model in models.values():
        model.markov_safe = True

    horizon = 24
    loan = {
        "ORIGINAL_UPB": 200_000.0,
        "ORIGINAL_INTEREST_RATE": 6.25,
        "ORIGINAL_LOAN_TERM": 360.0,
        "CURRENT_INTEREST_RATE": 6.25,
        "x1": 0.3,
        "x2": -0.4,
    }
    projected = project_loan(
        models, cfg, loan,
        Scenario(pl.DataFrame({"MONTHLY_REPORTING_PERIOD": ["200703"]})),
        horizon=horizon, start_period="200703",
    )

    # The same loan, many times over, with the identical covariates.
    n = 40_000
    loans = _loans(n).with_columns(
        pl.lit(0.3).alias("x1"), pl.lit(-0.4).alias("x2")
    )
    result = simulate(
        models, cfg, loans, None, horizon=horizon, seed=17, start_period="200703"
    )

    space = StateSpace.from_config(cfg)
    final = result.monthly.filter(pl.col("PERIOD") == result.monthly["PERIOD"].max())
    simulated = {
        row["STATE"]: row["N"] / n for row in final.iter_rows(named=True) if row["N"]
    }

    for state in space.transient:
        expected = float(projected.cumulative(state)[-1])
        got = simulated.get(state, 0.0)
        # ~3 standard errors at n=40,000, plus a floor for the very rare states.
        assert abs(got - expected) < 4 * (expected * (1 - expected) / n) ** 0.5 + 0.002, (
            f"{state}: matrix {expected:.4f} vs simulated {got:.4f}"
        )


def test_no_loan_transitions_twice_in_one_month(cfg, toy_models):
    """Routing must read the month's STARTING state.

    Scoring in place lets a path move CURRENT -> DLQ_30 under the `00` model and
    then be picked up again by `01` in the same month, cascading all the way to
    CHARGEOFF in a single step -- which produced charge-offs two months after
    origination, an event no single model's destination set allows.
    """
    result = simulate_chunk(
        toy_models, cfg, _loans(3_000), None, horizon=1, seed=3
    )[0]
    reachable = set(cfg.destinations("00")) | {"CURRENT"}
    assert set(result["STATE"].to_list()) <= reachable


def test_absorbing_states_never_release_paths(cfg, toy_models):
    monthly = simulate_chunk(toy_models, cfg, _loans(500), None, horizon=12, seed=4)[0]
    space = StateSpace.from_config(cfg)
    stock = monthly.filter(pl.col("STATE").is_in(list(space.absorbing)))
    # Absorbing states are flows only: a path that enters is gone, so it must
    # never show up in the active stock.
    assert stock["N"].sum() == 0


def test_active_population_only_ever_shrinks(cfg, toy_models):
    monthly = simulate_chunk(toy_models, cfg, _loans(800), None, horizon=18, seed=5)[0]
    active = monthly.group_by("PERIOD").agg(pl.col("N").sum()).sort("PERIOD")["N"].to_list()
    assert all(b <= a for a, b in zip(active, active[1:])), active


# --- determinism -----------------------------------------------------------


def test_same_seed_reproduces_the_run(cfg, toy_models):
    kw = dict(horizon=8, seed=99, replicates=1)
    a = simulate(toy_models, cfg, _loans(400), None, **kw).monthly
    b = simulate(toy_models, cfg, _loans(400), None, **kw).monthly
    assert a.equals(b)


def test_results_are_invariant_to_chunk_size(cfg, toy_models):
    """Chunking is an operational knob and must not change the answer.

    A per-chunk RNG would silently violate this, which is why the uniforms come
    from a hash of (seed, path, month) instead.
    """
    kw = dict(horizon=8, seed=42, replicates=1)
    whole = simulate(toy_models, cfg, _loans(600), None, chunk_paths=10_000, **kw).monthly
    split = simulate(toy_models, cfg, _loans(600), None, chunk_paths=97, **kw).monthly
    whole, split = whole.sort("PERIOD", "STATE"), split.sort("PERIOD", "STATE")

    # The sampled paths must be bit-identical, so every count matches exactly.
    assert whole["PERIOD"].to_list() == split["PERIOD"].to_list()
    assert whole["STATE"].to_list() == split["STATE"].to_list()
    for column in ("N", "N_ENTERED"):
        assert whole[column].to_list() == split[column].to_list(), column

    # Money is summed in a different order, and floating-point addition is not
    # associative -- so equality holds to precision, not to the bit.
    for column in ("UPB", "UPB_ENTERED", "LOSS"):
        assert (whole[column] - split[column]).abs().max() < 1e-6, column


def test_replicates_multiply_the_simulated_population(cfg, toy_models):
    one = simulate(toy_models, cfg, _loans(200), None, horizon=4, seed=8, replicates=1)
    three = simulate(toy_models, cfg, _loans(200), None, horizon=4, seed=8, replicates=3)
    assert three.n_paths == 3 * one.n_paths
    assert three.monthly["N"].sum() > one.monthly["N"].sum()


# --- severity --------------------------------------------------------------


def _severity_observations(n=4_000, seed=0) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    return pl.DataFrame(
        {
            "LOSS_SEVERITY": np.concatenate(
                [rng.normal(0.25, 0.1, n // 2), rng.normal(0.65, 0.15, n // 2)]
            ),
            "MARK_TO_MARKET_LTV": np.concatenate(
                [rng.uniform(50, 80, n // 2), rng.uniform(100, 150, n // 2)]
            ),
            "PROPERTY_STATE": ["CA"] * (n // 2) + ["TX"] * (n // 2),
        }
    )


def test_empirical_severity_reproduces_its_own_distribution():
    model = EmpiricalSeverity.fit(_severity_observations(), by_state=False)
    frame = pl.DataFrame(
        {"MARK_TO_MARKET_LTV": np.full(20_000, 130.0), "PROPERTY_STATE": ["CA"] * 20_000}
    )
    drawn = model.draw(frame, uniforms(np.arange(20_000, dtype=np.uint64), 1, 3))
    # The 130 LTV band is the high-severity population, not the pooled average.
    assert 0.55 < float(np.mean(drawn)) < 0.75


def test_severity_varies_with_equity():
    """The whole reason to condition: deeper negative equity recovers less."""
    model = EmpiricalSeverity.fit(_severity_observations(), by_state=False)
    u = uniforms(np.arange(5_000, dtype=np.uint64), 1, 3)
    low = model.draw(
        pl.DataFrame({"MARK_TO_MARKET_LTV": np.full(5_000, 60.0), "PROPERTY_STATE": ["CA"] * 5_000}), u
    )
    high = model.draw(
        pl.DataFrame({"MARK_TO_MARKET_LTV": np.full(5_000, 140.0), "PROPERTY_STATE": ["CA"] * 5_000}), u
    )
    assert float(np.mean(high)) > float(np.mean(low)) + 0.2


def test_thin_buckets_fall_back_to_the_pooled_distribution():
    """A quantile function built from a handful of observations is noise, and the
    simulation would resample that noise for every path in the bucket."""
    obs = _severity_observations(n=200)
    model = EmpiricalSeverity.fit(obs, by_state=True, min_observations=10_000)
    assert model.buckets == {}
    drawn = model.draw(
        pl.DataFrame({"MARK_TO_MARKET_LTV": [70.0], "PROPERTY_STATE": ["CA"]}),
        np.array([0.5]),
    )
    assert np.isclose(drawn[0], np.median(model.fallback), atol=0.05)


def test_severity_requires_realised_observations():
    with pytest.raises(SeverityError):
        EmpiricalSeverity.fit(pl.DataFrame({"MARK_TO_MARKET_LTV": [90.0]}))


def test_flat_severity_matches_the_matrix_assumption():
    frame = pl.DataFrame({"MARK_TO_MARKET_LTV": [90.0, 120.0]})
    assert FlatSeverity(0.5).draw(frame, np.array([0.1, 0.9])).tolist() == [0.5, 0.5]


def test_losses_only_arise_in_declared_loss_states(cfg, toy_models):
    monthly = simulate_chunk(
        toy_models, cfg, _loans(2_000), None, horizon=24, seed=6, severity=0.5
    )[0]
    losing = set(monthly.filter(pl.col("LOSS") > 0)["STATE"].to_list())
    assert losing <= set(("CHARGEOFF", "REO_DISPOSITION", "CREDIT_EVENT_OTHER"))


# --- macro -----------------------------------------------------------------


def _macro_panel() -> MacroPanel:
    return MacroPanel.build(
        national=pl.DataFrame(
            {"MONTHLY_REPORTING_PERIOD": ["200703", "200704"], "MORTGAGE_RATE_30Y": [6.2, 6.4]}
        ),
        state=pl.DataFrame(
            {
                "MONTHLY_REPORTING_PERIOD": ["200703", "200703", "200704", "200704"],
                "GEO_CODE": ["CA", "TX", "CA", "TX"],
                "HPI_STATE": [640.0, 220.0, 630.0, 222.0],
            }
        ),
    )


def test_macro_resolves_each_loan_against_its_own_state():
    """Index levels are not comparable across states -- California sat near 640
    in 2007 while Texas was near 220. Collapsing them mis-marks equity for most
    of the book."""
    panel = _macro_panel()
    frame = pl.DataFrame({"PROPERTY_STATE": ["CA", "TX", "CA"]})
    out = panel.attach(frame, "200703")
    assert out["HPI_STATE"].to_list() == [640.0, 220.0, 640.0]
    assert out["MORTGAGE_RATE_30Y"].to_list() == [6.2, 6.2, 6.2]


def test_macro_holds_the_last_known_value_past_the_panel():
    """A forecast horizon routinely runs beyond macro history; nulls there would
    blank every macro feature for the rest of the run."""
    panel = _macro_panel()
    assert panel.effective_period("209912") == "200704"
    out = panel.attach(pl.DataFrame({"PROPERTY_STATE": ["CA"]}), "209912")
    assert out["HPI_STATE"].to_list() == [630.0]


def test_macro_replaces_rather_than_suffixes_existing_columns():
    """A join would suffix the incumbent column and the model would keep reading
    the stale original."""
    panel = _macro_panel()
    frame = pl.DataFrame({"PROPERTY_STATE": ["CA"], "HPI_STATE": [999.0]})
    out = panel.attach(frame, "200704")
    assert "HPI_STATE_right" not in out.columns
    assert out["HPI_STATE"].to_list() == [630.0]


def test_scenario_still_resolves_a_single_state():
    scenario = Scenario(
        pl.DataFrame({"MONTHLY_REPORTING_PERIOD": ["200703"], "HPI_STATE": [640.0]})
    )
    assert scenario.row_for("200703")["HPI_STATE"] == 640.0
    assert scenario.row_for("209912")["HPI_STATE"] == 640.0
