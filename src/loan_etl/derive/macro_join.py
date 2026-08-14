"""Attach the macro panel to loan-months, point-in-time correctly.

The join key is ``AVAILABLE_PERIOD`` -- the month an observation could first
have been used -- not the month it describes. A loan-month for 2008-03 picks up
the unemployment reading that had actually been published by 2008-03, which is
the February print, first release.

The previous implementation joined on the observation month using the latest
vintage, so a 2008 training row saw an unemployment number that was neither
published nor final until well after the fact.
"""

from __future__ import annotations

import polars as pl

from ..settings import Settings

STATE_SERIES = "UNEMPLOYMENT_STATE"
HPI_SERIES = "HPI_STATE"


def load_macro(settings: Settings) -> pl.DataFrame:
    path = settings.curated / "macro_monthly.parquet"
    if not path.exists():
        raise FileNotFoundError(
            f"No macro panel at {path}. Run the 'macro' stage first."
        )
    return pl.read_parquet(path)


def _wide_national(panel: pl.DataFrame, include_pit_unsafe: bool) -> pl.DataFrame:
    nat = panel.filter(pl.col("GEO_LEVEL") == "national")
    if not include_pit_unsafe:
        nat = nat.filter(~pl.col("PIT_UNSAFE"))
    if nat.height == 0:
        return pl.DataFrame({"AVAILABLE_PERIOD": []}, schema={"AVAILABLE_PERIOD": pl.Utf8})
    return nat.pivot(
        on="SERIES_NAME", index="AVAILABLE_PERIOD", values="VALUE", aggregate_function="last"
    )


def _wide_state(panel: pl.DataFrame) -> pl.DataFrame:
    """Every state-level series, pivoted wide on (period, state).

    Pivots whatever state series the panel carries rather than naming one. An
    earlier version hardcoded UNEMPLOYMENT_STATE, so adding HPI_STATE to the
    registry fetched it, stored it, and then silently dropped it on the join --
    the column simply never reached the panel.
    """
    st = panel.filter(pl.col("GEO_LEVEL") == "state")
    if st.height == 0:
        return pl.DataFrame(
            {"AVAILABLE_PERIOD": [], "GEO_CODE": [], STATE_SERIES: []},
            schema={"AVAILABLE_PERIOD": pl.Utf8, "GEO_CODE": pl.Utf8, STATE_SERIES: pl.Float64},
        )
    return st.pivot(
        on="SERIES_NAME",
        index=["AVAILABLE_PERIOD", "GEO_CODE"],
        values="VALUE",
        aggregate_function="last",
    )


def attach_macro(
    lf: pl.LazyFrame, settings: Settings, *, include_pit_unsafe: bool = False
) -> pl.LazyFrame:
    """Left-join national and state macro onto a loan-month frame.

    Left joins on purpose: a loan-month with no macro coverage (GU, VI) keeps
    its loan data and gets a null macro value, and ``gate_macro_coverage``
    reports it. An inner join -- what the previous notebook used -- would have
    silently deleted those loans from the training set.
    """
    panel = load_macro(settings)
    national = _wide_national(panel, include_pit_unsafe)
    state = _wide_state(panel)

    lf = lf.join(
        national.lazy(),
        left_on="MONTHLY_REPORTING_PERIOD",
        right_on="AVAILABLE_PERIOD",
        how="left",
    )
    lf = lf.join(
        state.lazy(),
        left_on=["MONTHLY_REPORTING_PERIOD", "PROPERTY_STATE"],
        right_on=["AVAILABLE_PERIOD", "GEO_CODE"],
        how="left",
    )

    # Refi incentive. Named INTEREST_RATE_DIFF for continuity with the existing
    # notebooks, but the definition is now note rate minus the published PMMS
    # 30-year rate, rather than minus a cohort average of origination rates.
    # The market rate is the economically meaningful benchmark and no longer
    # requires a self-join over the origination table.
    if "MORTGAGE_RATE_30Y" in national.columns:
        lf = lf.with_columns(
            (pl.col("CURRENT_INTEREST_RATE") - pl.col("MORTGAGE_RATE_30Y")).alias(
                "INTEREST_RATE_DIFF"
            )
        )
    else:
        lf = lf.with_columns(pl.lit(None, pl.Float64).alias("INTEREST_RATE_DIFF"))

    lf = _attach_home_equity(lf, panel)

    # UNEMPLOYMENT is the name the existing notebooks use. Kept as an alias so
    # they keep working; UNEMPLOYMENT_STATE is the unambiguous name, since the
    # panel also carries a national series.
    return lf.with_columns(pl.col(STATE_SERIES).alias("UNEMPLOYMENT"))


def _hpi_at_origination(panel: pl.DataFrame) -> pl.DataFrame:
    """State HPI level at each origination month.

    Joined on OBS_PERIOD, not AVAILABLE_PERIOD: the house price index prevailing
    when the loan was written is settled history by the time we are modelling
    month t, so there is no look-ahead in using the observed level. Only the
    CURRENT index needs the point-in-time treatment.
    """
    st = panel.filter(pl.col("SERIES_NAME") == HPI_SERIES)
    if st.height == 0:
        return pl.DataFrame(
            {"OBS_PERIOD": [], "GEO_CODE": [], "HPI_AT_ORIGINATION": []},
            schema={"OBS_PERIOD": pl.Utf8, "GEO_CODE": pl.Utf8, "HPI_AT_ORIGINATION": pl.Float64},
        )
    return st.select(
        "OBS_PERIOD", "GEO_CODE", pl.col("VALUE").alias("HPI_AT_ORIGINATION")
    ).unique(subset=["OBS_PERIOD", "GEO_CODE"], keep="last")


def _attach_home_equity(lf: pl.LazyFrame, panel: pl.DataFrame) -> pl.LazyFrame:
    """Mark-to-market LTV and the negative-equity flag.

    The single most important driver of crisis-era default, and the channel the
    panel previously had no way to express. Derivation:

        value_at_origination = ORIGINAL_UPB / (ORIGINAL_LTV / 100)
        value_now            = value_at_origination * HPI_now / HPI_at_origination
        MTM_LTV              = PRIOR_UPB / value_now * 100
                             = ORIGINAL_LTV * PRIOR_POOL_FACTOR
                                            * HPI_at_origination / HPI_now

    PRIOR_UPB (via PRIOR_POOL_FACTOR) rather than the current balance, so the
    feature stays knowable at the START of the month like every other feature.

    It is also Markov-safe: HPI_at_origination is a fixed loan attribute,
    HPI_now comes from the macro scenario, and the pool factor follows the
    amortisation schedule -- so all three can be advanced during projection.
    """
    if "HPI_AT_ORIGINATION" in lf.collect_schema().names():
        return lf

    orig_hpi = _hpi_at_origination(panel)
    lf = lf.join(
        orig_hpi.lazy(),
        left_on=["FIRST_PAYMENT_DATE", "PROPERTY_STATE"],
        right_on=["OBS_PERIOD", "GEO_CODE"],
        how="left",
    )

    hpi_now = pl.col(HPI_SERIES)
    hpi_orig = pl.col("HPI_AT_ORIGINATION")
    usable = hpi_now.is_not_null() & hpi_orig.is_not_null() & (hpi_orig > 0) & (hpi_now > 0)

    lf = lf.with_columns(
        pl.when(usable)
        .then(hpi_now / hpi_orig - 1.0)
        .otherwise(pl.lit(None, pl.Float64))
        .alias("HPI_GROWTH_SINCE_ORIGINATION")
    )
    lf = lf.with_columns(
        pl.when(usable & pl.col("PRIOR_POOL_FACTOR").is_not_null())
        .then(
            pl.col("ORIGINAL_LOAN_TO_VALUE")
            * pl.col("PRIOR_POOL_FACTOR")
            * hpi_orig
            / hpi_now
        )
        .otherwise(pl.lit(None, pl.Float64))
        .alias("MARK_TO_MARKET_LTV")
    )
    return lf.with_columns(
        (pl.col("MARK_TO_MARKET_LTV") > 100.0).alias("IS_NEGATIVE_EQUITY")
    )
