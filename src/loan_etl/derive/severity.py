"""Recoveries, loss and severity (LGD).

Freddie's sign conventions are the trap here: expenses are disclosed as
NEGATIVE numbers, recoveries as positive, and ACTUAL_LOSS_CALCULATION as
negative when a loss occurred. Getting one of those backwards produces a
plausible-looking severity that is simply wrong, which is why the loss is
reconstructed independently and reconciled against Freddie's own figure in
``validate.gate_severity_reconciliation``:

    loss = defaulted UPB + delinquent accrued interest + |expenses| - recoveries

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
        pl.sum_horizontal([pl.col(c).fill_null(0.0) for c in RECOVERY_COMPONENTS])
        .alias("TOTAL_RECOVERIES"),
        # Prefer Freddie's own total; fall back to the components when absent.
        pl.coalesce(pl.col("TOTAL_EXPENSES"), component_expenses)
        .abs()
        .alias("TOTAL_EXPENSES_SUM"),
    )

    # Recovery data is only meaningful once the loan has actually disposed.
    disposed = pl.col("ZERO_BALANCE_REMOVAL").is_not_null() & pl.col("IS_TERMINAL")
    proceeds_usable = pl.col("NET_SALE_PROCEEDS_CODE").is_null()

    reconstructed = -(
        pl.col("ZERO_BALANCE_REMOVAL").fill_null(0.0)
        + pl.col("DELINQUENT_ACCRUED_INTEREST").fill_null(0.0)
        + pl.col("TOTAL_EXPENSES_SUM").fill_null(0.0)
        - pl.col("TOTAL_RECOVERIES")
    )

    lf = lf.with_columns(
        pl.when(disposed & proceeds_usable)
        .then(reconstructed)
        .otherwise(pl.lit(None, pl.Float64))
        .alias("RECONSTRUCTED_LOSS"),
        pl.when(disposed)
        .then(pl.col("TOTAL_RECOVERIES") / pl.col("ZERO_BALANCE_REMOVAL"))
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

    # Severity uses Freddie's disclosed loss as ground truth. Negated so a loss
    # is a positive severity, and only defined where a loss actually occurred.
    loss_amount = -pl.col("ACTUAL_LOSS_CALCULATION")
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
        .then(loss_amount.clip(lower_bound=0.0))
        .otherwise(pl.lit(None, pl.Float64))
        .alias("LOSS_AMOUNT"),
    )

    return lf