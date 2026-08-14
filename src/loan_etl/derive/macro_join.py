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
    st = panel.filter((pl.col("GEO_LEVEL") == "state") & (pl.col("SERIES_NAME") == STATE_SERIES))
    if st.height == 0:
        return pl.DataFrame(
            {"AVAILABLE_PERIOD": [], "GEO_CODE": [], STATE_SERIES: []},
            schema={"AVAILABLE_PERIOD": pl.Utf8, "GEO_CODE": pl.Utf8, STATE_SERIES: pl.Float64},
        )
    return st.select(
        "AVAILABLE_PERIOD",
        "GEO_CODE",
        pl.col("VALUE").alias(STATE_SERIES),
    ).unique(subset=["AVAILABLE_PERIOD", "GEO_CODE"], keep="last")


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

    # UNEMPLOYMENT is the name the existing notebooks use. Kept as an alias so
    # they keep working; UNEMPLOYMENT_STATE is the unambiguous name, since the
    # panel also carries a national series.
    return lf.with_columns(pl.col(STATE_SERIES).alias("UNEMPLOYMENT"))
