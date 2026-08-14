"""FRED acquisition, publication lag, and the point-in-time macro join."""

from __future__ import annotations

import datetime as dt

import pandas as pd
import polars as pl
import pytest

from loan_etl.acquire.fred import (
    SeriesSpec,
    build_macro_panel,
    load_series_registry,
    to_monthly,
)
from loan_etl.derive.macro_join import attach_macro
from loan_etl.io import scan_dataset
from loan_etl.settings import SCHEMA_DIR

from .conftest import VINTAGE


class StubFred:
    """Deterministic stand-in for fredapi.Fred.

    first_release values differ from the latest vintage on purpose, so a test
    can tell which one the pipeline actually used.
    """

    def __init__(self, start="2006-01-01", periods=48):
        self.index = pd.date_range(start, periods=periods, freq="MS")

    def get_series(self, series_id: str):
        return pd.Series([9.0] * len(self.index), index=self.index)

    def get_series_first_release(self, series_id: str):
        return pd.Series([5.0] * len(self.index), index=self.index)


# --- registry --------------------------------------------------------------


def test_registry_includes_dc_and_pr():
    """DC was missing from the old 50-state list, nulling every DC loan."""
    specs, meta = load_series_registry(SCHEMA_DIR)
    state_codes = {s.geo_code for s in specs if s.geo_level == "state"}
    assert "DC" in state_codes
    assert "PR" in state_codes
    assert len(state_codes) >= 51
    assert set(meta["known_uncovered"]) == {"GU", "VI"}


def test_recession_indicator_flagged_pit_unsafe():
    specs, _ = load_series_registry(SCHEMA_DIR)
    usrec = next(s for s in specs if s.series_id == "USREC")
    assert usrec.pit_unsafe is True


# --- publication lag -------------------------------------------------------


def _spec(**kw):
    base = dict(
        series_id="X", name="X", geo_level="national", geo_code=None,
        frequency="monthly", agg="last", publication_lag_months=1,
        pit_mode="first_release",
    )
    base.update(kw)
    return SeriesSpec(**base)


def _long(dates, values, spec):
    return pl.DataFrame(
        {"OBS_DATE": dates, "VALUE": values},
        schema={"OBS_DATE": pl.Date, "VALUE": pl.Float64},
    ).with_columns(
        pl.lit(spec.series_id).alias("SERIES_ID"),
        pl.lit(spec.name).alias("SERIES_NAME"),
        pl.lit(spec.geo_level).alias("GEO_LEVEL"),
        pl.lit(spec.geo_code).alias("GEO_CODE"),
        pl.lit(spec.pit_mode).alias("PIT_MODE"),
    )


def test_publication_lag_shifts_availability_forward():
    """A March observation with a 1-month lag is available from April."""
    spec = _spec(publication_lag_months=1)
    df = _long([dt.date(2007, 3, 1)], [4.4], spec)
    out = to_monthly(df, spec)
    assert out["OBS_PERIOD"][0] == "200703"
    assert out["AVAILABLE_PERIOD"][0] == "200704"


def test_zero_lag_series_available_same_month():
    spec = _spec(publication_lag_months=0, pit_mode="last")
    df = _long([dt.date(2007, 3, 1)], [6.1], spec)
    assert to_monthly(df, spec)["AVAILABLE_PERIOD"][0] == "200703"


def test_daily_series_aggregated_to_monthly_mean():
    spec = _spec(frequency="daily", agg="mean", publication_lag_months=0)
    df = _long(
        [dt.date(2007, 3, 1), dt.date(2007, 3, 15), dt.date(2007, 3, 30)],
        [1.0, 2.0, 3.0],
        spec,
    )
    out = to_monthly(df, spec)
    assert out.height == 1
    assert out["VALUE"][0] == pytest.approx(2.0)


def test_quarterly_series_forward_filled_to_monthly():
    spec = _spec(frequency="quarterly", fill="forward", publication_lag_months=3)
    df = _long([dt.date(2007, 1, 1), dt.date(2007, 4, 1)], [100.0, 110.0], spec)
    out = to_monthly(df, spec).sort("OBS_MONTH")
    assert out.height == 4  # Jan..Apr
    assert out["VALUE"].to_list() == [100.0, 100.0, 100.0, 110.0]
    assert out["AVAILABLE_PERIOD"].to_list()[0] == "200704"


def test_first_release_preferred_over_latest_vintage(settings):
    """Revision bias: the stub returns 5.0 first-release vs 9.0 latest."""
    build_macro_panel(settings, StubFred(), throttle=False)
    panel = pl.read_parquet(settings.curated / "macro_monthly.parquet")
    unrate = panel.filter(pl.col("SERIES_NAME") == "UNEMPLOYMENT_NATIONAL")
    assert set(unrate["VALUE"]) == {5.0}

    # MORTGAGE30US declares pit_mode: last, so it should use the latest vintage.
    mtg = panel.filter(pl.col("SERIES_NAME") == "MORTGAGE_RATE_30Y")
    assert set(mtg["VALUE"]) == {9.0}


# --- join ------------------------------------------------------------------


def test_macro_join_is_point_in_time(bronze):
    """A loan-month must see the previous month's unemployment print."""
    build_macro_panel(bronze, StubFred(), throttle=False)

    from loan_etl.derive.panel import build_loan_month

    build_loan_month(bronze, VINTAGE, with_macro=True)
    d = scan_dataset(bronze.curated / "loan_month").collect()

    assert d["UNEMPLOYMENT_STATE"].null_count() < d.height
    assert d["UNEMPLOYMENT_NATIONAL"].drop_nulls().to_list()[0] == 5.0

    panel = pl.read_parquet(bronze.curated / "macro_monthly.parquet")
    march = panel.filter(
        (pl.col("SERIES_NAME") == "UNEMPLOYMENT_STATE")
        & (pl.col("GEO_CODE") == "IL")
        & (pl.col("AVAILABLE_PERIOD") == "200703")
    )
    assert march["OBS_PERIOD"][0] == "200702", "join used the same month, not the lagged print"


def test_pit_unsafe_series_excluded_by_default(bronze):
    build_macro_panel(bronze, StubFred(), throttle=False)
    from loan_etl.derive.panel import build_loan_month

    build_loan_month(bronze, VINTAGE, with_macro=True)
    cols = scan_dataset(bronze.curated / "loan_month").collect_schema().names()
    assert "RECESSION_NBER" not in cols

    build_loan_month(bronze, VINTAGE, with_macro=True, include_pit_unsafe=True)
    cols = scan_dataset(bronze.curated / "loan_month").collect_schema().names()
    assert "RECESSION_NBER" in cols


def test_uncovered_state_keeps_loan_but_nulls_macro(bronze):
    """A left join, not an inner one: the loan survives, the macro is null."""
    build_macro_panel(bronze, StubFred(), throttle=False)
    from loan_etl.derive.panel import build_loan_month

    build_loan_month(bronze, VINTAGE, with_macro=True)
    d = scan_dataset(bronze.curated / "loan_month").collect()
    # Every fixture loan is still present.
    assert d["LOAN_SEQUENCE_NUMBER"].n_unique() == 6

def test_downstream_notebook_columns_present(bronze):
    """The curated panel must remain a superset of what the notebooks consume.

    final_data_clean.ipynb / run_analysis.ipynb read these 15 names; the ETL
    rebuild is not allowed to break them.
    """
    build_macro_panel(bronze, StubFred(), throttle=False)
    from loan_etl.derive.panel import build_loan_month

    build_loan_month(bronze, VINTAGE, with_macro=True)
    cols = set(scan_dataset(bronze.curated / "loan_month").collect_schema().names())
    required = {
        "LOAN_SEQUENCE_NUMBER", "MONTHLY_REPORTING_PERIOD", "ORIGINAL_LOAN_TERM",
        "LOAN_AGE", "PROPERTY_STATE", "ORIGINAL_LOAN_TO_VALUE",
        "ORIGINAL_DEBT_TO_INCOME", "CREDIT_SCORE", "UNEMPLOYMENT", "PREPAID",
        "DELINQUENT", "INTEREST_RATE_DIFF", "ORIGINATION_YEAR",
        "ORIGINATION_MONTH", "ESTIMATED_LOAN_TO_VALUE",
    }
    assert required <= cols, f"missing: {sorted(required - cols)}"


def test_origination_year_present_without_macro(bronze):
    """These derive from origination alone and must not depend on the FRED key."""
    from loan_etl.derive.panel import build_loan_month

    build_loan_month(bronze, VINTAGE, with_macro=False)
    cols = set(scan_dataset(bronze.curated / "loan_month").collect_schema().names())
    assert {"ORIGINATION_YEAR", "ORIGINATION_MONTH"} <= cols
