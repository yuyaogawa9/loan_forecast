"""Panel state, competing-risks labels, and the leakage guard."""

from __future__ import annotations

import polars as pl
import pytest

from loan_etl.features import (
    FeatureError,
    load_registry,
    select,
    training_frame,
    unclassified_columns,
)
from loan_etl.io import scan_dataset
from loan_etl.validate import (
    ERROR,
    gate_all_columns_classified,
    gate_features_exclude_leakage,
    gate_lagged_state_is_causal,
)

from .conftest import VINTAGE


def _panel(settings):
    return scan_dataset(settings.curated / "loan_month")


# --- lagged state ----------------------------------------------------------


def test_prior_dlq_equals_previous_month(curated):
    d = _panel(curated).collect()
    l3 = d.filter(pl.col("LOAN_SEQUENCE_NUMBER") == "F07Q10000003").sort("LOAN_AGE")
    dlq = l3["DLQ_MONTHS"].to_list()
    prior = l3["PRIOR_DLQ_MONTHS"].to_list()
    assert prior[0] is None
    assert prior[1:] == dlq[:-1]


def test_delinquency_run_length_accumulates(curated):
    d = _panel(curated).collect()
    l3 = d.filter(pl.col("LOAN_SEQUENCE_NUMBER") == "F07Q10000003").sort("LOAN_AGE")
    # Statuses 0,1,2,3,4,5,6,RA -> the spell starts at age 1, so the run length
    # observed at the start of each month is 0,0,1,2,3,4,5,6.
    assert l3["PRIOR_DLQ_RUN_LENGTH"].to_list() == [0, 0, 1, 2, 3, 4, 5, 6]


def test_max_dlq_to_date_excludes_current_month(curated):
    d = _panel(curated).collect()
    l3 = d.filter(pl.col("LOAN_SEQUENCE_NUMBER") == "F07Q10000003").sort("LOAN_AGE")
    assert l3["MAX_DLQ_MONTHS_TO_DATE"].to_list() == [None, 0, 1, 2, 3, 4, 5, 6]


def test_months_since_first_dlq_is_not_precognitive(curated):
    """Before the first delinquency the value must be null, not negative."""
    d = _panel(curated).collect()
    l3 = d.filter(pl.col("LOAN_SEQUENCE_NUMBER") == "F07Q10000003").sort("LOAN_AGE")
    assert l3["MONTHS_SINCE_FIRST_DLQ"].to_list() == [None, None, 1, 2, 3, 4, 5, 6]


def test_first_observation_has_no_prior_state(curated):
    d = _panel(curated).collect()
    first = d.filter(pl.col("IS_FIRST_OBSERVATION"))
    assert first.height == d["LOAN_SEQUENCE_NUMBER"].n_unique()
    assert first["PRIOR_UPB"].null_count() == first.height
    assert first["IS_MODELABLE"].sum() == 0


# --- event label -----------------------------------------------------------


def test_event_is_never_null(curated):
    """Every loan-month gets exactly one label, including the unknowable ones."""
    d = _panel(curated).collect()
    assert d["EVENT"].null_count() == 0


def test_unknown_event_only_where_source_status_was_unknown(curated):
    """'XX' is Freddie's unknown-status code; it must surface as UNKNOWN, not
    be quietly bucketed as CURRENT."""
    d = _panel(curated).collect()
    unknown = d.filter(pl.col("EVENT") == "UNKNOWN")
    assert unknown.height > 0
    # DLQ_MONTHS is null exactly where the raw status was the XX sentinel.
    assert unknown["DLQ_MONTHS"].null_count() == unknown.height
    assert unknown["IS_REO_ACQUISITION"].sum() == 0


def test_terminal_event_outranks_delinquency_state(curated):
    d = _panel(curated).collect()
    term = d.filter(pl.col("EVENT_TERMINAL").is_not_null())
    assert (term["EVENT"] == term["EVENT_TERMINAL"]).all()
    # Loan 5 is in REO acquisition on its final month AND disposes that month.
    l5 = d.filter(pl.col("LOAN_SEQUENCE_NUMBER") == "F07Q10000005").sort("LOAN_AGE")
    assert l5["DLQ_STATE"].to_list()[-1] == "REO"
    assert l5["EVENT"].to_list()[-1] == "REO_DISPOSITION"


def test_dlq_state_ladder(curated):
    d = _panel(curated).collect()
    l3 = d.filter(pl.col("LOAN_SEQUENCE_NUMBER") == "F07Q10000003").sort("LOAN_AGE")
    assert l3["DLQ_STATE"].to_list()[:5] == [
        "CURRENT", "DLQ_30", "DLQ_60", "DLQ_90_PLUS", "DLQ_90_PLUS",
    ]


# --- scheduled payment -----------------------------------------------------


def test_scheduled_payment_is_principal_plus_interest(curated):
    d = _panel(curated).filter(pl.col("IS_MODELABLE")).collect()
    row = d.filter(pl.col("SCHEDULED_PAYMENT").is_not_null()).to_dicts()[0]
    assert row["SCHEDULED_PAYMENT"] == pytest.approx(
        row["SCHEDULED_PRINCIPAL"] + row["SCHEDULED_INTEREST"], rel=1e-9
    )


def test_payment_outcome_flags_the_curtailment_month(curated):
    d = _panel(curated).collect()
    outcomes = set(d["PAYMENT_OUTCOME"])
    assert {"SCHEDULED", "CURTAILED", "TERMINAL", "FIRST_OBSERVATION"} <= outcomes
    curtailed = d.filter(pl.col("PAYMENT_OUTCOME") == "CURTAILED")
    assert curtailed.height == 1
    assert curtailed["LOAN_SEQUENCE_NUMBER"][0] == "F07Q10000006"


# --- registry / leakage ----------------------------------------------------


def test_no_unclassified_columns(curated):
    cols = _panel(curated).collect_schema().names()
    assert unclassified_columns(cols) == []


def test_selection_excludes_every_leakage_column(curated):
    cols = _panel(curated).collect_schema().names()
    reg = load_registry()
    sel = select(cols, "default")
    for c in sel.features:
        assert reg.role_of(c) not in ("target", "leakage"), c


@pytest.mark.parametrize(
    "leaky",
    ["ACTUAL_LOSS_CALCULATION", "ZERO_BALANCE_CODE", "CURRENT_ACTUAL_UPB",
     "CURRENT_LOAN_DELINQUENCY_STATUS", "DLQ_MONTHS"],
)
def test_specific_leakage_columns_never_become_features(curated, leaky):
    """These are the ones that would produce a perfect, useless model."""
    cols = _panel(curated).collect_schema().names()
    assert leaky in cols, "column should exist in the panel for auditing"
    sel = select(cols, "default")
    assert leaky not in sel.features


def test_target_itself_is_not_a_feature(curated):
    cols = _panel(curated).collect_schema().names()
    for alias in load_registry().target_groups:
        sel = select(cols, alias)
        assert sel.target not in sel.features


def test_training_frame_drops_first_observations(curated):
    panel = _panel(curated)
    lf, cols = training_frame(panel, "delinquency_30")
    out = lf.collect()
    assert out.height == panel.filter(pl.col("IS_MODELABLE")).select(pl.len()).collect().item()
    assert set(out.columns) == set(cols.identifiers) | set(cols.features) | {cols.target}


def test_unknown_target_raises(curated):
    cols = _panel(curated).collect_schema().names()
    with pytest.raises(FeatureError, match="Unknown target"):
        select(cols, "not_a_target")


def test_leakage_column_rejected_as_target(curated):
    """Asking to predict a leakage column is a mistake worth surfacing."""
    cols = _panel(curated).collect_schema().names()
    with pytest.raises(FeatureError, match="Unknown target"):
        select(cols, "ACTUAL_LOSS_CALCULATION")


def test_unclassified_column_blocks_selection(curated):
    cols = list(_panel(curated).collect_schema().names()) + ["MYSTERY_COLUMN"]
    with pytest.raises(FeatureError, match="no role in columns.yaml"):
        select(cols, "default")


def test_every_declared_target_group_resolves(curated):
    cols = _panel(curated).collect_schema().names()
    reg = load_registry()
    for alias in reg.target_groups:
        sel = select(cols, alias)
        assert sel.target in cols
        assert len(sel.features) > 0


# --- gates -----------------------------------------------------------------


def test_new_curated_gates_pass(curated):
    for gate in (
        gate_all_columns_classified,
        gate_features_exclude_leakage,
        gate_lagged_state_is_causal,
    ):
        r = gate(curated, VINTAGE)
        assert r.passed or r.severity != ERROR, str(r)


# --- model readiness -------------------------------------------------------


def test_no_raw_strings_survive_encoding(curated):
    """One leftover string makes XGBoost reject the entire frame.

    Regression guard: the categorical set is derived from dtypes, not from the
    hand-maintained YAML list, which had missed four string columns.
    """
    lf, cols = training_frame(_panel(curated), "delinquency_90")
    schema = lf.collect_schema()
    leftover = [c for c in cols.features if schema[c] == pl.Utf8]
    assert leftover == [], leftover


def test_categorical_features_are_encoded(curated):
    lf, cols = training_frame(_panel(curated), "default")
    schema = lf.collect_schema()
    for c in cols.categorical:
        assert schema[c] == pl.Categorical, c


def test_degenerate_features_are_dropped(curated):
    """All-null categoricals have zero levels and crash XGBoost's category path."""
    lf, cols = training_frame(_panel(curated), "default")
    assert "SUPER_CONFORMING_FLAG" in cols.dropped_degenerate
    assert "SUPER_CONFORMING_FLAG" not in cols.features
    df = lf.collect()
    for c in cols.features:
        assert df[c].drop_nulls().n_unique() > 1, c


def test_degenerate_drop_can_be_disabled(curated):
    _, cols = training_frame(_panel(curated), "default", drop_degenerate=False)
    assert cols.dropped_degenerate == ()
    assert "SUPER_CONFORMING_FLAG" in cols.features


def test_frame_actually_trains_an_xgboost_model(curated):
    """End-to-end proof the panel is model-ready, not just well-typed."""
    xgb = pytest.importorskip("xgboost")
    lf, cols = training_frame(_panel(curated), "delinquency_90")
    df = lf.collect()
    X = df.select(cols.features).to_pandas()
    y = df[cols.target].to_pandas().astype(int)
    model = xgb.XGBClassifier(
        n_estimators=5, max_depth=2, enable_categorical=True, tree_method="hist"
    )
    model.fit(X, y)
    assert model.predict_proba(X).shape == (df.height, 2)


def test_starting_state_conditions_on_prior_status(curated):
    from loan_etl.features import starting_state

    out = starting_state(_panel(curated), "00").collect()
    assert out.height > 0
    assert set(out["PRIOR_DLQ_STATUS"]) == {"00"}
