"""Path-history parity: the simulator must rebuild features the way the ETL did.

This is the gate for Monte Carlo simulation. Nothing downstream is trustworthy
unless the history recursion in `loan_model.paths` reproduces
`loan_etl.derive.events.with_panel_state` exactly, because any divergence hands
the model a covariate distribution it was never trained on -- a failure mode that
produces plausible probabilities and no error.

The test does not check the rules restated in prose. It replays every loan's
REAL observed delinquency sequence through the simulator's own recursion and
compares against the columns the ETL actually wrote.
"""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from loan_etl.io import scan_dataset
from loan_model.paths import (
    DLQ_CARRY,
    DLQ_NULL,
    PATH_FEATURES,
    DlqCoding,
    PathHistory,
    period_index,
    status_strings,
)
from loan_model.states import load_transition_config
from loan_model.transition import StateSpace

def replay_panel(panel: pl.DataFrame) -> pl.DataFrame:
    """Replay observed sequences for every loan in lockstep, N paths wide.

    Deliberately drives the same vectorised recursion the engine uses rather
    than a scalar reimplementation -- a per-loan reference loop could agree with
    the ETL while the array code the simulator actually runs did not.
    """
    grouped = (
        panel.sort(["LOAN_SEQUENCE_NUMBER", "MONTHLY_REPORTING_PERIOD"])
        .group_by("LOAN_SEQUENCE_NUMBER", maintain_order=True)
        .agg(
            pl.col("DLQ_MONTHS").fill_null(DLQ_NULL).cast(pl.Int32),
            pl.col("MONTHLY_REPORTING_PERIOD"),
            pl.col("IS_MODIFIED").fill_null(False),
        )
    )
    ids = grouped["LOAN_SEQUENCE_NUMBER"].to_list()
    seqs = grouped["DLQ_MONTHS"].to_list()
    pers = grouped["MONTHLY_REPORTING_PERIOD"].to_list()
    mods = grouped["IS_MODIFIED"].to_list()

    k, t_max = len(seqs), max(len(s) for s in seqs)
    dlq = np.full((k, t_max), DLQ_NULL, dtype=np.int32)
    month = np.zeros((k, t_max), dtype=np.int32)
    modified = np.zeros((k, t_max), dtype=bool)
    valid = np.zeros((k, t_max), dtype=bool)
    period = [["" for _ in range(t_max)] for _ in range(k)]
    for i, (s, p, m) in enumerate(zip(seqs, pers, mods)):
        dlq[i, : len(s)] = s
        month[i, : len(p)] = period_index(p)
        modified[i, : len(m)] = m
        valid[i, : len(s)] = True
        period[i][: len(p)] = p

    history = PathHistory.fresh(k)
    out = []
    for t in range(t_max):
        frame = pl.DataFrame(
            {
                "LOAN_SEQUENCE_NUMBER": ids,
                "MONTHLY_REPORTING_PERIOD": [row[t] for row in period],
                **history.features(month[:, t]),
            }
        ).filter(pl.Series(valid[:, t]))
        out.append(frame)
        history.advance(
            dlq[:, t], month[:, t], update=valid[:, t], modified_now=modified[:, t]
        )
    return pl.concat(out)


@pytest.fixture(scope="module")
def cfg():
    return load_transition_config()


# --- the gate --------------------------------------------------------------


def test_replayed_history_matches_the_etl_exactly(curated):
    """Every path feature, every modelable loan-month, on real derived data.

    Restricted to rows with a resolvable FROM_STATE because those -- and only
    those -- are the rows the models are trained on and the simulator ever
    reproduces. A row whose prior status was "XX" is dropped from training by
    `panel_with_states`, so parity there is not required.
    """
    cfg = load_transition_config()
    panel = scan_dataset(curated.curated / "loan_month").collect()
    replayed = replay_panel(panel)

    actual = (
        panel.with_columns(cfg.from_state_expr())
        .filter(pl.col("IS_MODELABLE") & pl.col("FROM_STATE").is_not_null())
        .select("LOAN_SEQUENCE_NUMBER", "MONTHLY_REPORTING_PERIOD", *PATH_FEATURES)
    )
    assert actual.height > 0, "fixture produced no modelable rows to compare"

    joined = actual.join(
        replayed,
        on=["LOAN_SEQUENCE_NUMBER", "MONTHLY_REPORTING_PERIOD"],
        how="inner",
        suffix="_sim",
    )
    assert joined.height == actual.height, "replay did not cover every modelable row"

    mismatched = {}
    for col in PATH_FEATURES:
        # ne_missing treats null == null as equal; a plain != returns null there
        # and would silently pass rows where one side is null and the other is not.
        bad = joined.filter(pl.col(col).ne_missing(pl.col(f"{col}_sim")))
        if bad.height:
            mismatched[col] = bad.select(
                "LOAN_SEQUENCE_NUMBER", "MONTHLY_REPORTING_PERIOD", col, f"{col}_sim"
            ).head(5).to_dicts()
    assert not mismatched, f"path features diverge from the ETL: {mismatched}"


def test_the_fixture_actually_exercises_deep_delinquency_and_reo(curated):
    """Guards the gate above: parity over trivial data would prove nothing.

    The fixture walks a loan 00 -> 01 -> ... -> 06 -> RA, which is what makes the
    90+ counter and the REO run-break observable at all.
    """
    panel = scan_dataset(curated.curated / "loan_month").collect()
    assert panel.filter(pl.col("DLQ_MONTHS") >= 4).height > 0, "no deep delinquency"
    assert panel.filter(pl.col("IS_REO_ACQUISITION")).height > 0, "no REO month"
    assert panel.filter(pl.col("PRIOR_DLQ_RUN_LENGTH") >= 3).height > 0, "no long spell"


# --- the two behaviours that look like defects -----------------------------


def test_reo_breaks_the_delinquency_run_and_does_not_count():
    """An REO month is not a delinquent month, because "RA" does not parse.

    Preserved deliberately. The ETL's `is_dlq` is False there, so the spell
    counter `(~is_dlq).cum_sum()` increments and the run resets. Simulating it
    any other way would be more intuitive and would not match training.
    """
    history = PathHistory.fresh(1)
    age = np.array([0], dtype=np.int32)
    for months in (1, 2, 3):
        history.advance(np.array([months], dtype=np.int32), age)
    assert history.run_length[0] == 3
    assert history.n_dlq[0] == 3

    history.advance(np.array([DLQ_NULL], dtype=np.int32), age)
    assert history.run_length[0] == 0, "REO must break the run"
    assert history.n_dlq[0] == 3, "REO must not count as a delinquent month"
    assert history.max_dlq[0] == 3, "REO must not disturb the running maximum"


def test_ninety_plus_keeps_counting_past_the_collapse_threshold():
    """The state is lumped at 3; the month counter is not.

    This is the whole reason to simulate. A first-order chain pins every 90+ loan
    at the same state and applies one average cure rate, which is what makes
    projected credit events come out roughly 6x too low.
    """
    cfg = load_transition_config()
    space = StateSpace.from_config(cfg)
    coding = DlqCoding.from_config(space, cfg)
    deep = np.array([space.index("DLQ_90_PLUS")])

    assert coding.next_months(deep, np.array([2], dtype=np.int32))[0] == 3
    assert coding.next_months(deep, np.array([3], dtype=np.int32))[0] == 4
    assert coding.next_months(deep, np.array([23], dtype=np.int32))[0] == 24


def test_status_codes_stay_inside_the_fitted_encoder_alphabet():
    """The matrix path's actual bug, pinned so simulation cannot repeat it.

    `project._covariates_for_state` feeds the pooled model the literal bucket
    label "03_PLUS". Encoders are fitted on "03".."99", so it falls through to
    MISSING_CODE and deletes the strongest predictor in the state where
    charge-offs originate.
    """
    codes = status_strings(np.array([0, 1, 3, 24, 99, 150, DLQ_NULL]))
    assert list(codes) == ["00", "01", "03", "24", "99", "99", "RA"]
    assert all(c == "RA" or (c.isdigit() and len(c) == 2) for c in codes)


def test_dlq_coding_is_derived_from_the_config_not_hardcoded(cfg):
    space = StateSpace.from_config(cfg)
    coding = DlqCoding.from_config(space, cfg)

    assert coding.fixed[space.index("CURRENT")] == 0
    assert coding.fixed[space.index("DLQ_30")] == 1
    assert coding.fixed[space.index("DLQ_60")] == 2
    assert coding.fixed[space.index("REO")] == DLQ_NULL
    assert coding.deep[space.index("DLQ_90_PLUS")]
    # Absorbing states say nothing about delinquency: carry, never overwrite.
    for state in cfg.absorbing_states:
        assert coding.fixed[space.index(state)] == DLQ_CARRY
        assert not coding.deep[space.index(state)]


def test_absorbing_destinations_carry_the_previous_count(cfg):
    space = StateSpace.from_config(cfg)
    coding = DlqCoding.from_config(space, cfg)
    prepaid = np.array([space.index("PREPAID")])
    assert coding.next_months(prepaid, np.array([7], dtype=np.int32))[0] == 7


# --- seeding ---------------------------------------------------------------


def test_history_seeds_from_a_live_panel_rather_than_assuming_a_clean_record():
    """Starting a seasoned portfolio at zero would tell the model every loan had
    never missed a payment."""
    df = pl.DataFrame(
        {
            "MONTHLY_REPORTING_PERIOD": ["202401", "202401"],
            "PRIOR_DLQ_MONTHS": [2, None],
            "PRIOR_DLQ_RUN_LENGTH": [2, 0],
            "MAX_DLQ_MONTHS_TO_DATE": [4, 1],
            "N_DLQ_MONTHS_TO_DATE": [9, 1],
            "MONTHS_SINCE_FIRST_DLQ": [30, None],
            "PRIOR_IS_MODIFIED": [True, False],
        }
    )
    start = period_index(df["MONTHLY_REPORTING_PERIOD"])
    history = PathHistory.from_panel(df)
    assert history.dlq_months.tolist() == [2, DLQ_NULL]
    assert history.max_dlq.tolist() == [4, 1]
    assert history.n_dlq.tolist() == [9, 1]
    assert history.first_dlq_month.tolist() == [int(start[0]) - 30, -1]
    assert history.is_modified.tolist() == [True, False]

    feats = history.features(start + 1)
    assert feats["MONTHS_SINCE_FIRST_DLQ"].to_list() == [31, None]
    assert feats["PRIOR_DLQ_STATUS"].to_list() == ["02", "RA"]


def test_months_since_first_dlq_is_null_until_the_event_is_strictly_past():
    """The ETL guards with `first_dlq_age < LOAN_AGE`; without it the feature
    reads the very event being predicted."""
    history = PathHistory.fresh(1)
    age = np.array([10], dtype=np.int32)
    assert history.features(age)["MONTHS_SINCE_FIRST_DLQ"].to_list() == [None]

    history.advance(np.array([1], dtype=np.int32), age)
    assert history.features(age)["MONTHS_SINCE_FIRST_DLQ"].to_list() == [None]
    assert history.features(np.array([11], dtype=np.int32))[
        "MONTHS_SINCE_FIRST_DLQ"
    ].to_list() == [1]


def test_absorbed_paths_freeze_their_history():
    history = PathHistory.fresh(2)
    age = np.array([5, 5], dtype=np.int32)
    history.advance(
        np.array([1, 1], dtype=np.int32), age, update=np.array([True, False])
    )
    assert history.n_dlq.tolist() == [1, 0]
    assert history.run_length.tolist() == [1, 0]
