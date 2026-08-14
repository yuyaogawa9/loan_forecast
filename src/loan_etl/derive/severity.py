"""Recoveries, loss and severity (LGD).

Sign conventions, verified against the actual Release 47 data rather than
assumed -- an earlier version of this module had all three backwards, which the
reconciliation gate caught:

    recoveries              NEGATIVE (they reduce the loss)
    expenses                POSITIVE (they increase it)
    ACTUAL_LOSS_CALCULATION POSITIVE when a loss occurred

Because recoveries already carry their own sign, the reconstruction is a plain
sum with no negation anywhere:

    loss = ZERO_BALANCE_REMOVAL + DELINQUENT_ACCRUED_INTEREST
         + TOTAL_EXPENSES + TOTAL_RECOVERIES

That reproduces Freddie's own figure exactly -- to the cent, not merely within
tolerance -- which is what ``validate.gate_severity_reconciliation`` asserts.

NET_SALE_PROCEEDS carries the non-numeric codes C and U. "C" means proceeds
covered the loss; "U" means the amount is unknown. In neither case is a numeric
recovery available, so the reconstruction is set to null rather than treating a
missing value as zero -- which would manufacture a large phantom loss.
"""

from __future__ import annotations

import polars as pl

EXPENSE_COMPONENTS = [
    "LEGAL_COSTS",
    "MAINTENANCE_AND_PRESERVATION_COSTS",
    "TAXES_AND_INSURANCE",
    "MISCELLANEOUS_EXPENSES",
]

RECOVERY_COMPONENTS = ["MI_RECOVERIES", "NET_SALE_PROCEEDS_AMT", "NON_MI_RECOVERIES"]


def with_severity(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Attach recovery, loss and severity columns to a loan-month frame."""
    component_expenses = pl.sum_horizontal(
        [pl.col(c).fill_null(0.0) for c in EXPENSE_COMPONENTS]
    )

    lf = lf.with_columns(
        # Signed as disclosed (negative). Do NOT flip: the reconstruction below
        # relies on them carrying their own sign.
        pl.sum_horizontal([pl.col(c).fill_null(0.0) for c in RECOVERY_COMPONENTS])
        .alias("TOTAL_RECOVERIES"),
        # Prefer Freddie's own total; fall back to the components when absent.
        # No .abs() -- expenses are disclosed positive and add to the loss.
        pl.coalesce(pl.col("TOTAL_EXPENSES"), component_expenses)
        .alias("TOTAL_EXPENSES_SUM"),
    )

    # Recovery data is only meaningful once the loan has actually disposed.
    disposed = pl.col("ZERO_BALANCE_REMOVAL").is_not_null() & pl.col("IS_TERMINAL")
    proceeds_usable = pl.col("NET_SALE_PROCEEDS_CODE").is_null()

    reconstructed = (
        pl.col("ZERO_BALANCE_REMOVAL").fill_null(0.0)
        + pl.col("DELINQUENT_ACCRUED_INTEREST").fill_null(0.0)
        + pl.col("TOTAL_EXPENSES_SUM").fill_null(0.0)
        + pl.col("TOTAL_RECOVERIES")
    )

    lf = lf.with_columns(
        pl.when(disposed & proceeds_usable)
        .then(reconstructed)
        .otherwise(pl.lit(None, pl.Float64))
        .alias("RECONSTRUCTED_LOSS"),
        # Negated so a recovery rate reads as a positive fraction of the
        # defaulted balance, the conventional orientation.
        pl.when(disposed & (pl.col("ZERO_BALANCE_REMOVAL") > 0))
        .then(-pl.col("TOTAL_RECOVERIES") / pl.col("ZERO_BALANCE_REMOVAL"))
        .otherwise(pl.lit(None, pl.Float64))
        .alias("RECOVERY_RATE"),
        pl.when(pl.col("NET_SALE_PROCEEDS_CODE") == "C")
        .then(pl.lit("PROCEEDS_COVERED_LOSS"))
        .when(pl.col("NET_SALE_PROCEEDS_CODE") == "U")
        .then(pl.lit("PROCEEDS_UNKNOWN"))
        .when(disposed)
        .then(pl.lit("NUMERIC"))
        .otherwise(pl.lit(None, pl.Utf8))
        .alias("RECOVERY_QUALITY"),
    )

    # Freddie's disclosed loss is already positive when a loss occurred, so it
    # is used as-is. Severity is therefore a positive fraction of the defaulted
    # balance, and a negative value means the disposition produced a gain.
    loss_amount = pl.col("ACTUAL_LOSS_CALCULATION")
    lf = lf.with_columns(
        pl.when(
            disposed
            & pl.col("ACTUAL_LOSS_CALCULATION").is_not_null()
            & (pl.col("ZERO_BALANCE_REMOVAL") > 0)
        )
        .then(loss_amount / pl.col("ZERO_BALANCE_REMOVAL"))
        .otherwise(pl.lit(None, pl.Float64))
        .alias("LOSS_SEVERITY"),
        pl.when(disposed)
        .then(loss_amount)
        .otherwise(pl.lit(None, pl.Float64))
        .alias("LOSS_AMOUNT"),
    )

    return lf