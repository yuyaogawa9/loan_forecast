"""Delinquency, default, charge-off and prepayment events.

Zero-balance code semantics drive most of this, and the distinctions matter:

    01  Prepaid or Matured -- voluntary payoff. Prepayment, NOT a credit event.
    02  Third-party sale          } credit events: the loan left the book
    03  Short sale or charge-off  } because it went bad. Loss data disclosed.
    09  REO disposition           }
    15  Note / whole-loan sale    }
    16  Reperforming loan securitisation -- NO loss data is disclosed, so this
        cannot be labelled either way. Censored.
    96  Repurchase for an underwriting or servicing defect -- the loan left for
        reasons unrelated to borrower credit. Censored.

Treating 16 or 96 as "not defaulted" would bias default rates downward, which
is why they are censored rather than folded into the negative class.
"""

from __future__ import annotations

import polars as pl

VOLUNTARY_PAYOFF_CODES = ["01"]
CREDIT_EVENT_CODES = ["02", "03", "09", "15"]
CHARGEOFF_CODES = ["03"]
REO_CODES = ["09"]
CENSOR_CODES = ["16", "96"]

SDQ_THRESHOLD_MONTHS = 6  # 180+ days past due


def with_event_flags(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Attach monthly event flags to a performance / loan-month frame."""
    status = pl.col("CURRENT_LOAN_DELINQUENCY_STATUS")
    zb = pl.col("ZERO_BALANCE_CODE")

    lf = lf.with_columns(
        # "RA" and "XX" are not numbers. RA is surfaced as its own flag; XX was
        # already nulled by the schema. Casting the raw column, as the previous
        # pipeline did, silently destroyed both.
        status.cast(pl.Int16, strict=False).alias("DLQ_MONTHS"),
        (status == "RA").fill_null(False).alias("IS_REO_ACQUISITION"),
    )

    dlq = pl.col("DLQ_MONTHS")
    lf = lf.with_columns(
        (dlq >= 1).fill_null(False).alias("IS_DLQ_30"),
        (dlq >= 2).fill_null(False).alias("IS_DLQ_60"),
        (dlq >= 3).fill_null(False).alias("IS_DLQ_90"),
        (dlq >= SDQ_THRESHOLD_MONTHS).fill_null(False).alias("IS_SDQ"),
        zb.is_in(CREDIT_EVENT_CODES).fill_null(False).alias("IS_CREDIT_EVENT"),
        zb.is_in(CHARGEOFF_CODES).fill_null(False).alias("IS_CHARGEOFF"),
        zb.is_in(REO_CODES).fill_null(False).alias("IS_REO_DISPOSITION"),
        zb.is_in(CENSOR_CODES).fill_null(False).alias("IS_CENSORED"),
        zb.is_not_null().alias("IS_TERMINAL"),
        pl.col("MODIFICATION_FLAG").is_in(["Y", "P"]).fill_null(False).alias("IS_MODIFIED"),
    )

    # A voluntary payoff before the scheduled maturity date is a prepayment;
    # reaching maturity is not. Both carry zero-balance code 01.
    prepaid = (
        zb.is_in(VOLUNTARY_PAYOFF_CODES)
        & pl.col("ZERO_BALANCE_EFFECTIVE_DATE").is_not_null()
        & pl.col("MATURITY_DATE").is_not_null()
        & (pl.col("ZERO_BALANCE_EFFECTIVE_DATE") < pl.col("MATURITY_DATE"))
    )
    lf = lf.with_columns(
        prepaid.fill_null(False).alias("IS_PREPAID_FULL"),
        (
            zb.is_in(VOLUNTARY_PAYOFF_CODES) & ~prepaid.fill_null(False)
        ).fill_null(False).alias("IS_MATURED"),
    )

    # Monthly default indicator: seriously delinquent, in REO, or terminated by
    # a credit event.
    lf = lf.with_columns(
        (
            pl.col("IS_SDQ") | pl.col("IS_REO_ACQUISITION") | pl.col("IS_CREDIT_EVENT")
        ).alias("IS_DEFAULT"),
        # Retained for continuity with the existing notebooks, which define
        # DELINQUENT as "not current and not in an active repayment plan".
        (
            pl.col("IS_DLQ_30") | pl.col("IS_REO_ACQUISITION")
        ).cast(pl.Int8).alias("DELINQUENT"),
        pl.col("IS_PREPAID_FULL").cast(pl.Int8).alias("PREPAID"),
    )
    return lf


def _period_index(period: pl.Expr) -> pl.Expr:
    """YYYYMM -> a monotone month counter, so periods can be differenced."""
    return period.str.slice(0, 4).cast(pl.Int32) * 12 + period.str.slice(4, 2).cast(pl.Int32)


def with_panel_state(
    lf: pl.LazyFrame,
    *,
    loan_key: str = "LOAN_SEQUENCE_NUMBER",
    order_key: str = "MONTHLY_REPORTING_PERIOD",
) -> pl.LazyFrame:
    """Lagged loan state: what was known at the START of each month.

    This is the difference between a panel you can model and one you cannot.
    ``CURRENT_LOAN_DELINQUENCY_STATUS`` at month t is the *outcome* of month t,
    so using it to predict ``IS_DLQ_30`` at t is just reading the answer. Every
    column here is shifted or cumulative-through-t-1, so it is safe as a feature
    for an event occurring during t.

    Anything not prefixed PRIOR_ is still causal: cumulative measures are taken
    up to and excluding the current month.
    """
    over = dict(partition_by=loan_key, order_by=order_key)
    dlq = pl.col("DLQ_MONTHS")
    is_dlq = (dlq >= 1).fill_null(False)

    lf = lf.with_columns(
        dlq.shift(1).over(**over).alias("PRIOR_DLQ_MONTHS"),
        pl.col("CURRENT_LOAN_DELINQUENCY_STATUS").shift(1).over(**over).alias("PRIOR_DLQ_STATUS"),
        pl.col("IS_MODIFIED").shift(1).over(**over).alias("PRIOR_IS_MODIFIED"),
        # cum_max includes the current row, so shift afterwards to exclude it.
        dlq.fill_null(0).cum_max().over(**over).shift(1).over(**over).alias("MAX_DLQ_MONTHS_TO_DATE"),
        is_dlq.cum_sum().over(**over).shift(1).over(**over).alias("N_DLQ_MONTHS_TO_DATE"),
    )

    # Consecutive delinquent months ending at t-1. The reset counter increments
    # on every current month, so each delinquency spell forms its own group and
    # a running count within that group is the spell length.
    reset = (~is_dlq).cum_sum().over(**over)
    lf = lf.with_columns(reset.alias("_DLQ_SPELL_ID"))
    lf = lf.with_columns(
        is_dlq.cast(pl.Int32)
        .cum_sum()
        .over(partition_by=[loan_key, "_DLQ_SPELL_ID"], order_by=order_key)
        .shift(1)
        .over(**over)
        .fill_null(0)
        .alias("PRIOR_DLQ_RUN_LENGTH")
    )

    # Months since the loan first went delinquent, measured on the REPORTING
    # PERIOD rather than LOAN_AGE.
    #
    # Taking the minimum over the whole loan partition is only causally safe if
    # the key is monotonic, because the `< current` guard is what excludes future
    # events. LOAN_AGE is NOT monotonic: a modification resets it, and Freddie
    # restates it from the modified terms. Ranking on it therefore let a future
    # delinquency masquerade as a past one -- loan F07Q10169987 is modified in
    # 200911 while 30 days down and its age resets 32 -> 1, so `min(LOAN_AGE)`
    # over delinquent rows returned 1 and the feature reported "1 month since
    # first delinquency" back in 200704, five months BEFORE the loan first
    # missed a payment. That affected 255,344 modelable loan-months (2.77% of
    # the rows where this feature is populated), concentrated in modified loans
    # -- which are exactly the crisis-vintage loans that drive credit losses.
    #
    # The reporting period is monotonic by construction, so the same guard is
    # sound against it, and the elapsed count is in real calendar months rather
    # than a counter the servicer can reset.
    period_index = _period_index(pl.col(order_key))
    first_dlq_period = period_index.filter(is_dlq).min().over(partition_by=loan_key)
    lf = lf.with_columns(
        pl.when(first_dlq_period < period_index)
        .then((period_index - first_dlq_period).cast(pl.Int32))
        .otherwise(pl.lit(None, pl.Int32))
        .alias("MONTHS_SINCE_FIRST_DLQ")
    )

    lf = lf.with_columns(
        (pl.col("MAX_DLQ_MONTHS_TO_DATE") >= 1).fill_null(False).alias("EVER_DLQ_30_TO_DATE"),
        (pl.col("MAX_DLQ_MONTHS_TO_DATE") >= 3).fill_null(False).alias("EVER_DLQ_90_TO_DATE"),
        (pl.col("PRIOR_UPB") / pl.col("ORIGINAL_UPB")).alias("PRIOR_POOL_FACTOR"),
        pl.col("PRIOR_UPB").is_null().alias("IS_FIRST_OBSERVATION"),
        # Rows whose lagged features exist. Hazard models should filter on this;
        # the first observation of each loan has no prior state by construction.
        pl.col("PRIOR_UPB").is_not_null().alias("IS_MODELABLE"),
    )
    return lf.drop("_DLQ_SPELL_ID")


def with_event_label(lf: pl.LazyFrame) -> pl.LazyFrame:
    """One mutually-exclusive outcome per loan-month, for competing risks.

    The boolean flags overlap by design (a charge-off is also a credit event and
    may also be seriously delinquent), which is fine for binary models but
    unusable for a multinomial or competing-risks fit. These three columns give
    a single label at three granularities.
    """
    lf = lf.with_columns(
        pl.when(pl.col("IS_REO_ACQUISITION"))
        .then(pl.lit("REO"))
        .when(pl.col("DLQ_MONTHS") >= 3)
        .then(pl.lit("DLQ_90_PLUS"))
        .when(pl.col("DLQ_MONTHS") == 2)
        .then(pl.lit("DLQ_60"))
        .when(pl.col("DLQ_MONTHS") == 1)
        .then(pl.lit("DLQ_30"))
        .when(pl.col("DLQ_MONTHS") == 0)
        .then(pl.lit("CURRENT"))
        .otherwise(pl.lit(None, pl.Utf8))
        .alias("DLQ_STATE"),
        pl.when(pl.col("IS_CENSORED"))
        .then(pl.lit("CENSORED"))
        .when(pl.col("IS_CHARGEOFF"))
        .then(pl.lit("CHARGEOFF"))
        .when(pl.col("IS_REO_DISPOSITION"))
        .then(pl.lit("REO_DISPOSITION"))
        .when(pl.col("IS_CREDIT_EVENT"))
        .then(pl.lit("CREDIT_EVENT_OTHER"))
        .when(pl.col("IS_PREPAID_FULL"))
        .then(pl.lit("PREPAID"))
        .when(pl.col("IS_MATURED"))
        .then(pl.lit("MATURED"))
        .otherwise(pl.lit(None, pl.Utf8))
        .alias("EVENT_TERMINAL"),
    )
    # Terminal events win: a loan that pays off while 30 days down is a
    # prepayment, not a delinquency observation.
    return lf.with_columns(
        pl.coalesce(pl.col("EVENT_TERMINAL"), pl.col("DLQ_STATE"), pl.lit("UNKNOWN")).alias("EVENT")
    )


def _first_period_where(flag: str, alias: str) -> pl.Expr:
    return (
        pl.col("MONTHLY_REPORTING_PERIOD")
        .filter(pl.col(flag))
        .min()
        .alias(alias)
    )


def build_loan_outcomes(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Collapse the loan-month panel to one row per loan.

    Convenient for survival / competing-risks models, where the unit of
    observation is the loan and the quantities of interest are time-to-event.
    """
    return (
        lf.group_by("LOAN_SEQUENCE_NUMBER")
        .agg(
            pl.col("vintage_year").first(),
            pl.col("PROPERTY_STATE").first(),
            pl.col("CREDIT_SCORE").first(),
            pl.col("ORIGINAL_UPB").first(),
            pl.col("ORIGINAL_LOAN_TERM").first(),
            pl.col("ORIGINAL_INTEREST_RATE").first(),
            pl.col("ORIGINAL_LOAN_TO_VALUE").first(),
            pl.col("ORIGINAL_DEBT_TO_INCOME").first(),
            pl.len().alias("MONTHS_OBSERVED"),
            pl.col("MONTHLY_REPORTING_PERIOD").min().alias("FIRST_PERIOD"),
            pl.col("MONTHLY_REPORTING_PERIOD").max().alias("LAST_PERIOD"),
            pl.col("LOAN_AGE").max().alias("MAX_LOAN_AGE"),
            # Terminal state
            pl.col("ZERO_BALANCE_CODE").drop_nulls().last().alias("ZERO_BALANCE_CODE"),
            pl.col("ZERO_BALANCE_EFFECTIVE_DATE").drop_nulls().last().alias("TERMINATION_PERIOD"),
            pl.col("LOAN_AGE").filter(pl.col("IS_TERMINAL")).max().alias("AGE_AT_TERMINATION"),
            pl.col("ZERO_BALANCE_REMOVAL").drop_nulls().last().alias("ZERO_BALANCE_REMOVAL"),
            # Ever-flags
            pl.col("IS_DLQ_30").any().alias("EVER_DLQ_30"),
            pl.col("IS_DLQ_60").any().alias("EVER_DLQ_60"),
            pl.col("IS_DLQ_90").any().alias("EVER_DLQ_90"),
            pl.col("IS_SDQ").any().alias("EVER_SDQ"),
            pl.col("IS_DEFAULT").any().alias("EVER_DEFAULT"),
            pl.col("IS_MODIFIED").any().alias("EVER_MODIFIED"),
            # First occurrence, for time-to-event
            _first_period_where("IS_DLQ_30", "FIRST_DLQ_30_PERIOD"),
            _first_period_where("IS_DLQ_90", "FIRST_DLQ_90_PERIOD"),
            _first_period_where("IS_SDQ", "FIRST_SDQ_PERIOD"),
            _first_period_where("IS_DEFAULT", "FIRST_DEFAULT_PERIOD"),
            pl.col("LOAN_AGE").filter(pl.col("IS_DEFAULT")).min().alias("AGE_AT_FIRST_DEFAULT"),
            # Terminal classification
            pl.col("IS_PREPAID_FULL").any().alias("IS_PREPAID_FULL"),
            pl.col("IS_MATURED").any().alias("IS_MATURED"),
            pl.col("IS_CHARGEOFF").any().alias("IS_CHARGEOFF"),
            pl.col("IS_REO_DISPOSITION").any().alias("IS_REO_DISPOSITION"),
            pl.col("IS_CREDIT_EVENT").any().alias("IS_CREDIT_EVENT"),
            pl.col("IS_CENSORED").any().alias("IS_CENSORED"),
            # Curtailment activity
            pl.col("CURTAILMENT").sum().alias("TOTAL_CURTAILMENT"),
            pl.col("IS_PARTIAL_PREPAYMENT").sum().alias("N_PARTIAL_PREPAYMENTS"),
            # Loss components, carried from the disposition record
            pl.col("ACTUAL_LOSS_CALCULATION").drop_nulls().last().alias("ACTUAL_LOSS_CALCULATION"),
            pl.col("TOTAL_RECOVERIES").drop_nulls().last().alias("TOTAL_RECOVERIES"),
            pl.col("TOTAL_EXPENSES_SUM").drop_nulls().last().alias("TOTAL_EXPENSES_SUM"),
            pl.col("RECONSTRUCTED_LOSS").drop_nulls().last().alias("RECONSTRUCTED_LOSS"),
            pl.col("LOSS_SEVERITY").drop_nulls().last().alias("LOSS_SEVERITY"),
            pl.col("NET_SALE_PROCEEDS_CODE").drop_nulls().last().alias("NET_SALE_PROCEEDS_CODE"),
        )
        .with_columns(
            pl.when(pl.col("IS_CENSORED"))
            .then(pl.lit("CENSORED"))
            .when(pl.col("IS_CHARGEOFF"))
            .then(pl.lit("CHARGEOFF"))
            .when(pl.col("IS_REO_DISPOSITION"))
            .then(pl.lit("REO_DISPOSITION"))
            .when(pl.col("IS_CREDIT_EVENT"))
            .then(pl.lit("CREDIT_EVENT_OTHER"))
            .when(pl.col("IS_PREPAID_FULL"))
            .then(pl.lit("PREPAID"))
            .when(pl.col("IS_MATURED"))
            .then(pl.lit("MATURED"))
            .otherwise(pl.lit("ACTIVE"))
            .alias("TERMINAL_OUTCOME")
        )
    )