"""Scheduled amortization, curtailment (partial prepayment), SMM and CPR.

Curtailment is measured as actual principal reduction in excess of what the
loan's own payment schedule required:

    i_t                = CURRENT_INTEREST_RATE(t-1) / 1200
    rem_t              = REMAINING_MONTHS_TO_LEGAL_MATURITY(t-1)
    scheduled_principal = UPB(t-1) * i_t * [ 1 / (1 - (1+i_t)^-rem_t) - 1 ]
    actual_principal    = UPB(t-1) - UPB(t)
    curtailment         = actual_principal - scheduled_principal

The schedule is recomputed each month from the *current* rate and remaining
term rather than from origination terms. That self-corrects for modifications,
which change rate and/or term mid-life and would otherwise make every
post-modification month look like a large curtailment.

Reliability caveats, all surfaced as CURTAILMENT_RELIABLE rather than silently
folded into the number:

* Freddie rounds CURRENT_ACTUAL_UPB for some vintages. If it is rounded to the
  nearest $1,000 then any curtailment smaller than that is pure quantisation
  noise. ``detect_upb_rounding`` measures the actual granularity in the data
  instead of assuming; the ingest records the result in the manifest.
* Payment deferral moves principal into a non-interest-bearing bucket. That is
  not a prepayment, but it does reduce the interest-bearing balance.
* Interest-only loans have zero scheduled principal during the IO period.
* The terminal month is a payoff, not a curtailment, and is excluded.
"""

from __future__ import annotations

import polars as pl

# A curtailment must clear BOTH an absolute floor and a share of the remaining
# balance to count. Neither alone is enough: a fixed floor is too loose on a
# $50k balance and too tight on a $700k one, and reporting noise scales with
# balance size. Anything below the threshold is indistinguishable from rounding
# and payment-timing drift, and treating it as a prepayment produces a partial
# prepayment in nearly every loan-month.
DEFAULT_ABS_FLOOR = 100.0
DEFAULT_REL_FLOOR = 0.0005  # 5 bp of prior balance
MONTHS_PER_YEAR = 12


def detect_upb_rounding(lf: pl.LazyFrame, column: str = "CURRENT_ACTUAL_UPB") -> dict[str, float]:
    """Empirically measure UPB granularity.

    Returns the share of non-zero balances that are exact multiples of $1,000
    and of $1. A high multiple-of-1000 share means curtailment below $1,000 is
    not measurable and CURTAILMENT_RELIABLE should be gated accordingly.
    """
    stats = (
        lf.filter(pl.col(column).is_not_null() & (pl.col(column) > 0))
        .select(
            pl.len().alias("n"),
            ((pl.col(column) % 1000) == 0).sum().alias("n_mult_1000"),
            ((pl.col(column) % 1) == 0).sum().alias("n_mult_1"),
        )
        .collect()
        .to_dicts()[0]
    )
    n = stats["n"] or 1
    return {
        "n_observations": stats["n"],
        "share_multiple_of_1000": stats["n_mult_1000"] / n,
        "share_multiple_of_1": stats["n_mult_1"] / n,
    }


def scheduled_balance(
    original_upb: pl.Expr, rate_pct: pl.Expr, term: pl.Expr, age: pl.Expr
) -> pl.Expr:
    """Closed-form remaining balance of a level-payment loan at a given age.

        B_t = P * [ (1+i)^n - (1+i)^t ] / [ (1+i)^n - 1 ]

    Satisfies B_0 == P and B_n == 0. The zero-rate case degenerates to straight
    line, handled separately because the formula is 0/0 there.
    """
    i = rate_pct / 1200.0
    one_plus_i = 1.0 + i
    pow_n = one_plus_i.pow(term)
    pow_t = one_plus_i.pow(age)
    general = original_upb * (pow_n - pow_t) / (pow_n - 1.0)
    straight_line = original_upb * (1.0 - age / term)
    return (
        pl.when(i <= 0)
        .then(straight_line)
        .otherwise(general)
        .clip(lower_bound=0.0)
    )


def scheduled_principal(prior_upb: pl.Expr, rate_pct: pl.Expr, remaining: pl.Expr) -> pl.Expr:
    """Principal portion of one level payment, given current state.

    Recursive form: takes the CURRENT balance and remaining term rather than
    origination terms, so it composes with a balance that has been reduced by
    curtailments. `scheduled_balance` is closed-form from origination and cannot
    -- which is why the path simulator advances balances through this.
    """
    i = rate_pct / 1200.0
    factor = 1.0 / (1.0 - (1.0 + i).pow(-remaining)) - 1.0
    return (
        pl.when((i <= 0) | (remaining <= 0))
        .then(pl.when(remaining > 0).then(prior_upb / remaining).otherwise(0.0))
        .otherwise(prior_upb * i * factor)
    )


def with_amortization(
    lf: pl.LazyFrame,
    *,
    abs_floor: float = DEFAULT_ABS_FLOOR,
    rel_floor: float = DEFAULT_REL_FLOOR,
    loan_key: str = "LOAN_SEQUENCE_NUMBER",
    order_key: str = "MONTHLY_REPORTING_PERIOD",
) -> pl.LazyFrame:
    """Attach scheduled/actual principal, curtailment, SMM and CPR.

    Expects a loan-month frame already joined to origination (needs
    ORIGINAL_UPB, ORIGINAL_LOAN_TERM, INTEREST_ONLY_INDICATOR).
    """
    over = dict(partition_by=loan_key, order_by=order_key)

    lf = lf.with_columns(
        pl.col("CURRENT_ACTUAL_UPB").shift(1).over(**over).alias("PRIOR_UPB"),
        pl.col("CURRENT_INTEREST_RATE").shift(1).over(**over).alias("PRIOR_RATE"),
        pl.col("REMAINING_MONTHS_TO_LEGAL_MATURITY")
        .shift(1)
        .over(**over)
        .alias("PRIOR_REMAINING_MONTHS"),
    )

    lf = lf.with_columns(
        scheduled_balance(
            pl.col("ORIGINAL_UPB"),
            pl.col("ORIGINAL_INTEREST_RATE"),
            pl.col("ORIGINAL_LOAN_TERM"),
            pl.col("LOAN_AGE"),
        ).alias("SCHEDULED_UPB_AT_ORIGINATION_TERMS"),
        scheduled_principal(
            pl.col("PRIOR_UPB"),
            pl.col("PRIOR_RATE"),
            pl.col("PRIOR_REMAINING_MONTHS"),
        ).alias("SCHEDULED_PRINCIPAL"),
        (pl.col("PRIOR_UPB") - pl.col("CURRENT_ACTUAL_UPB")).alias("ACTUAL_PRINCIPAL"),
    )

    # fill_null(False) on every flag is load-bearing, not defensive noise:
    # `is_in` and `==` both return NULL (not False) for a null input, and a null
    # anywhere in the reliability conjunction poisons the whole expression, so
    # every curtailment would silently be discarded as unreliable.
    is_interest_only = (pl.col("INTEREST_ONLY_INDICATOR") == "Y").fill_null(False)
    lf = lf.with_columns(
        pl.when(is_interest_only)
        .then(pl.lit(0.0))
        .otherwise(pl.col("SCHEDULED_PRINCIPAL"))
        .alias("SCHEDULED_PRINCIPAL")
    )

    is_terminal = pl.col("ZERO_BALANCE_CODE").is_not_null()
    is_modified = pl.col("MODIFICATION_FLAG").is_in(["Y", "P"]).fill_null(False)
    has_deferral = (
        pl.col("PAYMENT_DEFERRAL").is_not_null()
        | (pl.col("CURRENT_NON_INTEREST_BEARING_UPB").fill_null(0.0) > 0)
    ).fill_null(False)

    raw_curtailment = pl.col("ACTUAL_PRINCIPAL") - pl.col("SCHEDULED_PRINCIPAL")

    lf = lf.with_columns(
        pl.when(is_terminal)
        .then(pl.lit(None, pl.Float64))
        .otherwise(raw_curtailment.clip(lower_bound=0.0))
        .alias("CURTAILMENT"),
        (
            pl.col("PRIOR_UPB").is_not_null()
            & pl.col("CURRENT_ACTUAL_UPB").is_not_null()
            & ~is_terminal
            & ~is_modified
            & ~has_deferral
            & ~is_interest_only
        ).fill_null(False).alias("CURTAILMENT_RELIABLE"),
    )

    # Scheduled interest and the full scheduled P&I payment. Both are modelling
    # targets in their own right and the denominator for payment-behaviour work.
    lf = lf.with_columns(
        (pl.col("PRIOR_UPB") * pl.col("PRIOR_RATE") / 1200.0).alias("SCHEDULED_INTEREST")
    )
    lf = lf.with_columns(
        (pl.col("SCHEDULED_INTEREST") + pl.col("SCHEDULED_PRINCIPAL")).alias("SCHEDULED_PAYMENT")
    )

    lf = lf.with_columns(
        pl.max_horizontal(
            pl.lit(abs_floor), pl.col("PRIOR_UPB").fill_null(0.0) * rel_floor
        ).alias("CURTAILMENT_THRESHOLD")
    )
    is_material = (
        pl.col("CURTAILMENT_RELIABLE")
        & (pl.col("CURTAILMENT") > pl.col("CURTAILMENT_THRESHOLD"))
    ).fill_null(False)

    # Balance that would have survived the month absent any prepayment; the
    # denominator for the single monthly mortality rate.
    denom = pl.col("PRIOR_UPB") - pl.col("SCHEDULED_PRINCIPAL")
    lf = lf.with_columns(
        pl.when(is_material & (denom > 0))
        .then(pl.col("CURTAILMENT") / denom)
        .otherwise(pl.lit(0.0))
        .alias("SMM"),
        is_material.alias("IS_PARTIAL_PREPAYMENT"),
    )
    lf = lf.with_columns((1.0 - (1.0 - pl.col("SMM")).pow(MONTHS_PER_YEAR)).alias("CPR"))

    # Payment behaviour as a single mutually-exclusive outcome. SHORTFALL means
    # less principal was retired than the schedule required, which is what a
    # missed or partial payment looks like in balance terms.
    shortfall = pl.col("ACTUAL_PRINCIPAL") - pl.col("SCHEDULED_PRINCIPAL")
    lf = lf.with_columns(
        pl.when(is_terminal)
        .then(pl.lit("TERMINAL"))
        .when(pl.col("PRIOR_UPB").is_null())
        .then(pl.lit("FIRST_OBSERVATION"))
        .when(pl.col("IS_PARTIAL_PREPAYMENT"))
        .then(pl.lit("CURTAILED"))
        .when(shortfall < -pl.col("CURTAILMENT_THRESHOLD"))
        .then(pl.lit("SHORTFALL"))
        .otherwise(pl.lit("SCHEDULED"))
        .alias("PAYMENT_OUTCOME")
    )
    lf = lf.with_columns(
        (pl.col("PAYMENT_OUTCOME") == "SCHEDULED").alias("IS_SCHEDULED_PAYMENT")
    )

    return lf.drop("PRIOR_RATE", "PRIOR_REMAINING_MONTHS")