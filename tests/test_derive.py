"""Amortization, events, severity and the validation gates."""

from __future__ import annotations

import math

import polars as pl
import pytest

from loan_etl.derive.amortization import scheduled_balance
from loan_etl.io import scan_dataset
from loan_etl.validate import ERROR, validate_bronze, validate_curated

from .conftest import VINTAGE


# --- amortization ----------------------------------------------------------


def _sched(p, rate, term, age):
    return (
        pl.DataFrame({"p": [float(p)], "r": [float(rate)], "n": [float(term)], "t": [float(age)]})
        .select(
            scheduled_balance(pl.col("p"), pl.col("r"), pl.col("n"), pl.col("t")).alias("b")
        )["b"][0]
    )


def test_scheduled_balance_endpoints():
    """B_0 == principal and B_n == 0, for any rate."""
    assert _sched(200_000, 6.25, 360, 0) == pytest.approx(200_000, abs=1e-6)
    assert _sched(200_000, 6.25, 360, 360) == pytest.approx(0.0, abs=1e-6)


def test_scheduled_balance_matches_hand_computed_table():
    """Independent amortization loop must agree with the closed form."""
    p, rate, term = 200_000.0, 6.25, 360
    i = rate / 1200.0
    pmt = p * i / (1 - (1 + i) ** -term)
    bal = p
    for age in range(1, 13):
        bal -= pmt - bal * i
        assert _sched(p, rate, term, age) == pytest.approx(bal, abs=0.01)


def test_scheduled_balance_zero_rate_is_straight_line():
    assert _sched(120_000, 0.0, 120, 60) == pytest.approx(60_000, abs=1e-6)


def test_curtailment_detected_once_and_only_where_injected(curated):
    """The fixture injects exactly one $10k curtailment, on loan 6."""
    d = scan_dataset(curated.curated / "loan_month").collect()
    hits = d.filter(pl.col("IS_PARTIAL_PREPAYMENT"))
    assert hits.height == 1
    row = hits.to_dicts()[0]
    assert row["LOAN_SEQUENCE_NUMBER"] == "F07Q10000006"
    assert row["CURTAILMENT"] == pytest.approx(10_000, abs=5.0)
    assert 0 < row["SMM"] < 1
    assert row["CPR"] == pytest.approx(1 - (1 - row["SMM"]) ** 12, rel=1e-9)


def test_amortization_noise_does_not_register_as_prepayment(curated):
    """Sub-threshold drift must not be reported as partial prepayment.

    A naive absolute tolerance flags a curtailment in nearly every loan-month;
    this is the guard against that regression.
    """
    d = scan_dataset(curated.curated / "loan_month").collect()
    clean = d.filter(pl.col("LOAN_SEQUENCE_NUMBER") == "F07Q10000001")
    assert clean["IS_PARTIAL_PREPAYMENT"].sum() == 0


def test_terminal_month_excluded_from_curtailment(curated):
    d = scan_dataset(curated.curated / "loan_month").collect()
    terminal = d.filter(pl.col("IS_TERMINAL"))
    assert terminal.height > 0
    assert terminal["CURTAILMENT"].null_count() == terminal.height


# --- events ----------------------------------------------------------------


def test_terminal_outcome_classification(curated):
    o = scan_dataset(curated.curated / "loan_outcomes").collect()
    got = dict(zip(o["LOAN_SEQUENCE_NUMBER"], o["TERMINAL_OUTCOME"]))
    assert got["F07Q10000001"] == "PREPAID"
    assert got["F07Q10000003"] == "REO_DISPOSITION"
    assert got["F07Q10000004"] == "CHARGEOFF"
    assert got["F07Q10000005"] == "REO_DISPOSITION"
    assert got["F07Q10000006"] == "ACTIVE"


def test_delinquency_ladder_and_default(curated):
    d = scan_dataset(curated.curated / "loan_month").collect()
    l3 = d.filter(pl.col("LOAN_SEQUENCE_NUMBER") == "F07Q10000003").sort("LOAN_AGE")
    # Statuses run 0,1,2,3,4,5,6,RA -> SDQ at 6 months, REO on the last record.
    assert l3["IS_DLQ_30"].sum() == 6
    assert l3["IS_DLQ_90"].sum() == 4
    assert l3["IS_SDQ"].sum() == 1
    assert bool(l3["IS_REO_ACQUISITION"].tail(1)[0]) is True
    assert bool(l3["IS_DEFAULT"].any()) is True


def test_reo_acquisition_status_is_not_lost_to_numeric_cast(curated):
    """'RA' has no numeric value; DLQ_MONTHS is null but the flag is set."""
    d = scan_dataset(curated.curated / "loan_month").collect()
    ra = d.filter(pl.col("IS_REO_ACQUISITION"))
    assert ra.height == 2
    assert ra["DLQ_MONTHS"].null_count() == ra.height


def test_prepaid_requires_payoff_before_maturity(curated):
    d = scan_dataset(curated.curated / "loan_month").collect()
    prepaid = d.filter(pl.col("IS_PREPAID_FULL"))
    assert prepaid.height == 1
    row = prepaid.to_dicts()[0]
    assert row["ZERO_BALANCE_CODE"] == "01"
    assert row["ZERO_BALANCE_EFFECTIVE_DATE"] < row["MATURITY_DATE"]


# --- severity --------------------------------------------------------------


def test_reconstructed_loss_matches_freddie_figure(curated):
    """Independent reconstruction must reproduce ACTUAL_LOSS_CALCULATION.

    Sign conventions are the trap, and an earlier version had all three
    backwards: recoveries are disclosed NEGATIVE, expenses POSITIVE, and the
    loss POSITIVE. The reconstruction is therefore a plain sum.
    """
    d = scan_dataset(curated.curated / "loan_month").collect()
    row = d.filter(pl.col("LOAN_SEQUENCE_NUMBER") == "F07Q10000003").filter(
        pl.col("IS_TERMINAL")
    ).to_dicts()[0]
    assert row["TOTAL_RECOVERIES"] == pytest.approx(-171_000.0)  # disclosed negative
    assert row["TOTAL_EXPENSES_SUM"] == pytest.approx(12_000.0)  # disclosed positive
    assert row["RECONSTRUCTED_LOSS"] == pytest.approx(row["ACTUAL_LOSS_CALCULATION"], abs=0.01)
    assert row["LOSS_SEVERITY"] == pytest.approx(47_500.0 / 198_500.0, rel=1e-6)


def test_coded_proceeds_block_reconstruction_rather_than_zeroing_it(curated):
    """C/U mean the recovery is unquantified; a zero would fake a huge loss."""
    d = scan_dataset(curated.curated / "loan_month").collect()
    coded = d.filter(pl.col("NET_SALE_PROCEEDS_CODE").is_not_null())
    assert coded.height == 2
    assert coded["RECONSTRUCTED_LOSS"].null_count() == coded.height
    assert set(coded["RECOVERY_QUALITY"]) == {"PROCEEDS_COVERED_LOSS", "PROCEEDS_UNKNOWN"}
    # Severity still comes from Freddie's own figure, which is available.
    assert coded["LOSS_SEVERITY"].null_count() == 0


# --- gates -----------------------------------------------------------------


def test_all_bronze_gates_pass(bronze):
    results = validate_bronze(bronze, VINTAGE)
    failed = [r for r in results if not r.passed and r.severity == ERROR]
    assert not failed, [str(r) for r in failed]


def test_all_curated_gates_pass(curated):
    results = validate_curated(curated, VINTAGE)
    failed = [r for r in results if not r.passed and r.severity == ERROR]
    assert not failed, [str(r) for r in failed]


def test_referential_integrity_gate_catches_orphans(bronze):
    """Delete origination rows and confirm the gate actually fires."""
    from loan_etl.io import partition_file
    from loan_etl.validate import gate_referential_integrity

    path = partition_file(bronze.bronze / "origination", VINTAGE)
    df = pl.read_parquet(path).filter(pl.col("LOAN_SEQUENCE_NUMBER") != "F07Q10000003")
    df.write_parquet(path)

    result = gate_referential_integrity(bronze, VINTAGE)
    assert not result.passed
    assert result.metrics["orphan_loans"] == 1


def test_sentinel_gate_catches_leaked_credit_score(bronze):
    from loan_etl.io import partition_file
    from loan_etl.validate import gate_no_sentinel_leakage

    path = partition_file(bronze.bronze / "origination", VINTAGE)
    df = pl.read_parquet(path).with_columns(
        pl.when(pl.col("LOAN_SEQUENCE_NUMBER") == "F07Q10000001")
        .then(pl.lit(9999, pl.Int16))
        .otherwise(pl.col("CREDIT_SCORE"))
        .alias("CREDIT_SCORE")
    )
    df.write_parquet(path)

    result = gate_no_sentinel_leakage(bronze, VINTAGE)
    assert not result.passed
    assert "CREDIT_SCORE" in result.detail