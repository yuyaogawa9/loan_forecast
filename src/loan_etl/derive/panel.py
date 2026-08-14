"""Build the curated loan-month panel and the loan-level outcomes table.

Two curated outputs, both partitioned by vintage year:

``loan_month``    -- the all-encompassing panel: every cleaned origination and
                     performance field, plus event flags, amortization and
                     curtailment, loss and severity, and point-in-time macro.
                     One row per loan per reporting month.
``loan_outcomes`` -- one row per loan: terminal event, time to event, ever-flags
                     and realised loss. The convenient grain for survival and
                     competing-risks models.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from ..io import partition_file, scan_dataset, sink_partition, utc_now, write_manifest
from ..settings import Settings
from .amortization import DEFAULT_ABS_FLOOR, detect_upb_rounding, with_amortization
from .events import build_loan_outcomes, with_event_flags
from .macro_join import attach_macro
from .severity import with_severity

# Origination columns that would collide with performance columns on join.
_ORIG_DROP = ["vintage_year", "LOAN_VINTAGE_YEAR", "LOAN_VINTAGE_QUARTER"]


def _sink_or_collect(lf: pl.LazyFrame, base, year: int):
    """Stream if the engine can, otherwise materialise this vintage.

    Window functions over a loan partition are not always streamable. A single
    vintage of the Sample dataset is a few million rows, so the fallback is
    safe; it is the whole-dataset case that must never be materialised.
    """
    try:
        return sink_partition(lf, base, year)
    except Exception:
        out_dir = base / f"vintage_year={year}"
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / "data.parquet"
        lf.collect(engine="streaming").write_parquet(
            path, compression="zstd", compression_level=9
        )
        return path


def build_loan_month(
    settings: Settings,
    year: int,
    *,
    with_macro: bool = True,
    include_pit_unsafe: bool = False,
) -> dict[str, Any]:
    perf = scan_dataset(settings.bronze / "performance").filter(
        pl.col("vintage_year") == year
    )
    orig = (
        scan_dataset(settings.bronze / "origination")
        .filter(pl.col("vintage_year") == year)
        .drop(_ORIG_DROP, strict=False)
    )

    # Measure UPB granularity rather than assume it. If Freddie discloses this
    # vintage's balances rounded to the nearest $1,000, then no curtailment
    # below $1,000 is observable and the detection floor has to move with it --
    # otherwise quantisation noise is reported as partial prepayment.
    upb_rounding = detect_upb_rounding(perf)
    abs_floor = DEFAULT_ABS_FLOOR
    if upb_rounding["share_multiple_of_1000"] > 0.9:
        abs_floor = max(abs_floor, 1000.0)
    upb_rounding["applied_abs_floor"] = abs_floor

    lf = perf.join(orig, on="LOAN_SEQUENCE_NUMBER", how="left")
    lf = with_event_flags(lf)
    lf = with_amortization(lf, abs_floor=abs_floor)
    lf = with_severity(lf)

    # Derived from origination data alone, so they must exist whether or not
    # macro is attached -- the notebooks slice them out of FIRST_PAYMENT_DATE.
    lf = lf.with_columns(
        pl.col("FIRST_PAYMENT_DATE").str.slice(0, 4).alias("ORIGINATION_YEAR"),
        pl.col("FIRST_PAYMENT_DATE").str.slice(4, 2).alias("ORIGINATION_MONTH"),
    )

    if with_macro:
        lf = attach_macro(lf, settings, include_pit_unsafe=include_pit_unsafe)

    lf = lf.sort(["LOAN_SEQUENCE_NUMBER", "MONTHLY_REPORTING_PERIOD"])
    base = settings.curated / "loan_month"
    out_path = _sink_or_collect(lf, base, year)

    summary = (
        pl.scan_parquet(out_path)
        .select(
            pl.len().alias("rows"),
            pl.col("LOAN_SEQUENCE_NUMBER").n_unique().alias("loans"),
            pl.col("IS_DEFAULT").sum().alias("default_months"),
            pl.col("IS_PREPAID_FULL").sum().alias("full_prepayments"),
            pl.col("IS_PARTIAL_PREPAYMENT").sum().alias("partial_prepayments"),
            pl.col("IS_CHARGEOFF").sum().alias("chargeoffs"),
            pl.col("LOSS_SEVERITY").mean().alias("mean_loss_severity"),
        )
        .collect()
        .to_dicts()[0]
    )

    manifest = {
        "stage": "curated_loan_month",
        "vintage_year": year,
        "created_utc": utc_now(),
        "with_macro": with_macro,
        "include_pit_unsafe": include_pit_unsafe,
        "upb_rounding": upb_rounding,
        "summary": summary,
        "output_path": str(out_path),
        "output_bytes": out_path.stat().st_size,
    }
    write_manifest(settings.manifests, f"curated__loan_month__{year}", manifest)
    return manifest


def build_outcomes(settings: Settings, year: int) -> dict[str, Any]:
    lf = scan_dataset(settings.curated / "loan_month").filter(
        pl.col("vintage_year") == year
    )
    outcomes = build_loan_outcomes(lf)

    base = settings.curated / "loan_outcomes"
    out_path = _sink_or_collect(outcomes, base, year)

    dist = (
        pl.scan_parquet(out_path)
        .group_by("TERMINAL_OUTCOME")
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)
        .collect()
        .to_dicts()
    )
    manifest = {
        "stage": "curated_loan_outcomes",
        "vintage_year": year,
        "created_utc": utc_now(),
        "terminal_outcome_distribution": dist,
        "output_path": str(out_path),
        "output_bytes": out_path.stat().st_size,
    }
    write_manifest(settings.manifests, f"curated__loan_outcomes__{year}", manifest)
    return manifest


def curated_exists(settings: Settings, year: int) -> bool:
    return partition_file(settings.curated / "loan_month", year).exists()