"""FRED / ALFRED macro acquisition with point-in-time correctness.

Two independent sources of look-ahead bias are handled, and both must be, since
fixing one alone still leaks:

REVISION BIAS. ``fred.get_series`` returns the *latest* vintage. State
unemployment is revised substantially through annual benchmarking and seasonal
re-estimation, so joining today's view of 2008 onto 2008 loan-months feeds the
model numbers nobody had in 2008. ``pit_mode: first_release`` pulls the value as
originally published instead.

PUBLICATION LAG. Even the first print of month M is not out until roughly the
third week of M+1. Each series declares ``publication_lag_months``; the
observation is stamped with the period in which it *became available*
(``AVAILABLE_PERIOD``), and downstream joins on that rather than on the
observation date.

The output is long-form at (period, geo_level, geo_code, series) grain, one row
per observation, so the panel stays auditable and adding a series never changes
the schema.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import polars as pl
import yaml

from ..io import utc_now, write_manifest
from ..settings import Settings


class FredClient(Protocol):
    """Minimal surface used here; lets tests inject a stub."""

    def get_series(self, series_id: str) -> Any: ...
    def get_series_first_release(self, series_id: str) -> Any: ...


@dataclass(frozen=True)
class SeriesSpec:
    series_id: str
    name: str
    geo_level: str
    geo_code: str | None
    frequency: str
    agg: str
    publication_lag_months: int
    pit_mode: str
    fill: str | None = None
    pit_unsafe: bool = False
    desc: str = ""


class FredError(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


def load_series_registry(schema_dir: Path) -> tuple[list[SeriesSpec], dict[str, Any]]:
    spec = yaml.safe_load((schema_dir / "fred_series.yaml").read_text())
    defaults = spec.get("defaults", {})

    def _spec(raw: dict[str, Any], geo_code: str | None = None, sid: str | None = None) -> SeriesSpec:
        return SeriesSpec(
            series_id=sid or raw["id"],
            name=raw["name"],
            geo_level=raw["geo_level"],
            geo_code=geo_code,
            frequency=raw.get("frequency", "monthly"),
            agg=raw.get("agg", "last"),
            publication_lag_months=int(
                raw.get("publication_lag_months", defaults.get("publication_lag_months", 1))
            ),
            pit_mode=raw.get("pit_mode", defaults.get("pit_mode", "first_release")),
            fill=raw.get("fill"),
            pit_unsafe=bool(raw.get("pit_unsafe", False)),
            desc=(raw.get("desc") or "").strip(),
        )

    out = [_spec(raw) for raw in spec.get("series", [])]

    # State-level blocks are expanded once per state. Kept as a list so a new
    # geography-varying series (HPI, income, ...) is a config entry rather than
    # another special case in the loader.
    blocks = spec.get("state_series") or []
    if legacy := spec.get("state_unemployment"):
        blocks = [legacy, *blocks]

    uncovered: dict[str, list[str]] = {}
    for block in blocks:
        template = block["id_template"]
        for state in block["states"]:
            out.append(_spec(block, geo_code=state, sid=template.format(state=state)))
        uncovered[block["name"]] = list(block.get("known_uncovered") or [])

    meta = {
        "api": spec.get("api", {}),
        "known_uncovered_by_series": uncovered,
        # Union, for coverage checks that do not care which series is missing.
        "known_uncovered": sorted({s for v in uncovered.values() for s in v}),
    }
    return out, meta


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------


def _to_long(raw: Any, spec: SeriesSpec) -> pl.DataFrame:
    """pandas Series (date -> value) into a tidy polars frame."""
    dates = [d for d in raw.index]
    values = [None if v != v else float(v) for v in raw.values]  # NaN -> None
    return pl.DataFrame(
        {
            "OBS_DATE": [
                (d.date() if hasattr(d, "date") else d) for d in dates
            ],
            "VALUE": values,
        },
        schema={"OBS_DATE": pl.Date, "VALUE": pl.Float64},
    ).with_columns(
        pl.lit(spec.series_id).alias("SERIES_ID"),
        pl.lit(spec.name).alias("SERIES_NAME"),
        pl.lit(spec.geo_level).alias("GEO_LEVEL"),
        pl.lit(spec.geo_code).alias("GEO_CODE"),
    )


def fetch_series(
    client: FredClient, spec: SeriesSpec, *, max_retries: int, backoff_base: float
) -> pl.DataFrame:
    """Fetch one series honouring its declared PIT mode, with backoff."""
    last_err: Exception | None = None
    for attempt in range(max_retries):
        try:
            if spec.pit_mode == "first_release":
                try:
                    raw = client.get_series_first_release(spec.series_id)
                except Exception:
                    # Not every series exposes a vintage history. Fall back, but
                    # record that this series is latest-vintage so the leakage
                    # is visible rather than assumed away.
                    raw = client.get_series(spec.series_id)
                    return _to_long(raw, spec).with_columns(
                        pl.lit("last_fallback").alias("PIT_MODE")
                    )
            else:
                raw = client.get_series(spec.series_id)
            return _to_long(raw, spec).with_columns(pl.lit(spec.pit_mode).alias("PIT_MODE"))
        except Exception as exc:  # noqa: BLE001 -- retried and re-raised below
            last_err = exc
            time.sleep(backoff_base * (2**attempt))
    raise FredError(f"{spec.series_id}: failed after {max_retries} attempts: {last_err}")


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------


def to_monthly(df: pl.DataFrame, spec: SeriesSpec) -> pl.DataFrame:
    """Collapse to one row per calendar month using the declared aggregation."""
    monthly = df.with_columns(pl.col("OBS_DATE").dt.truncate("1mo").alias("OBS_MONTH"))
    agg = pl.col("VALUE").mean() if spec.agg == "mean" else pl.col("VALUE").last()
    out = (
        monthly.sort("OBS_DATE")
        .group_by("OBS_MONTH", "SERIES_ID", "SERIES_NAME", "GEO_LEVEL", "GEO_CODE", "PIT_MODE")
        .agg(agg.alias("VALUE"))
        .sort("OBS_MONTH")
    )

    if spec.fill == "forward":
        # Quarterly series only report on quarter starts; carry forward so every
        # month has a value, then the publication lag is applied on top.
        full = pl.date_range(
            out["OBS_MONTH"].min(), out["OBS_MONTH"].max(), interval="1mo", eager=True
        ).to_frame("OBS_MONTH")
        out = (
            full.join(out, on="OBS_MONTH", how="left")
            .sort("OBS_MONTH")
            .with_columns(
                pl.col("VALUE").forward_fill(),
                pl.col("SERIES_ID").forward_fill(),
                pl.col("SERIES_NAME").forward_fill(),
                pl.col("GEO_LEVEL").forward_fill(),
                pl.col("GEO_CODE").forward_fill(),
                pl.col("PIT_MODE").forward_fill(),
            )
            .drop_nulls("SERIES_ID")
        )

    # AVAILABLE_PERIOD is the month in which this observation could first have
    # been used. Everything downstream joins on this, never on OBS_MONTH.
    return out.with_columns(
        pl.col("OBS_MONTH")
        .dt.offset_by(f"{spec.publication_lag_months}mo")
        .alias("AVAILABLE_DATE")
    ).with_columns(
        pl.col("AVAILABLE_DATE").dt.strftime("%Y%m").alias("AVAILABLE_PERIOD"),
        pl.col("OBS_MONTH").dt.strftime("%Y%m").alias("OBS_PERIOD"),
        pl.lit(spec.publication_lag_months).alias("PUBLICATION_LAG_MONTHS"),
        pl.lit(spec.pit_unsafe).alias("PIT_UNSAFE"),
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def build_macro_panel(
    settings: Settings,
    client: FredClient | None = None,
    *,
    force: bool = False,
    throttle: bool = True,
) -> Path:
    """Fetch every registered series and write the long-form monthly panel.

    ``throttle=False`` skips the inter-request sleep; only for tests with a stub
    client, since the real API enforces a request rate.
    """
    specs, meta = load_series_registry(settings.schema_dir)
    api = meta.get("api", {})
    max_retries = int(api.get("max_retries", 5))
    backoff = float(api.get("backoff_base_seconds", 1.0))
    rate_limit = int(api.get("rate_limit_per_minute", 120))
    sleep_between = (60.0 / max(rate_limit, 1)) if throttle else 0.0

    if client is None:
        from fredapi import Fred

        client = Fred(api_key=settings.require_fred_key())

    settings.raw_fred.mkdir(parents=True, exist_ok=True)
    frames: list[pl.DataFrame] = []
    fetched, cached, failed = 0, 0, []

    for spec in specs:
        cache = settings.raw_fred / f"{spec.series_id}.parquet"
        if cache.exists() and not force:
            frames.append(pl.read_parquet(cache))
            cached += 1
            continue
        try:
            raw = fetch_series(client, spec, max_retries=max_retries, backoff_base=backoff)
        except FredError as exc:
            failed.append({"series_id": spec.series_id, "error": str(exc)})
            continue
        monthly = to_monthly(raw, spec)
        monthly.write_parquet(cache)
        frames.append(monthly)
        fetched += 1
        if sleep_between:
            time.sleep(sleep_between)

    if not frames:
        raise FredError("No FRED series could be fetched; macro panel not built.")

    panel = pl.concat(frames, how="vertical_relaxed").sort(
        ["GEO_LEVEL", "GEO_CODE", "SERIES_NAME", "AVAILABLE_PERIOD"]
    )
    out_path = settings.curated / "macro_monthly.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    panel.write_parquet(out_path, compression="zstd")

    write_manifest(
        settings.manifests,
        "macro_monthly",
        {
            "created_utc": utc_now(),
            "series_registered": len(specs),
            "series_fetched": fetched,
            "series_from_cache": cached,
            "series_failed": failed,
            "rows": panel.height,
            "period_range": [
                panel["AVAILABLE_PERIOD"].min(),
                panel["AVAILABLE_PERIOD"].max(),
            ],
            "pit_unsafe_series": sorted(
                panel.filter(pl.col("PIT_UNSAFE"))["SERIES_NAME"].unique().to_list()
            ),
            "latest_vintage_fallbacks": sorted(
                panel.filter(pl.col("PIT_MODE") == "last_fallback")["SERIES_NAME"]
                .unique()
                .to_list()
            ),
            "known_uncovered_states": meta.get("known_uncovered", []),
            "output_path": str(out_path),
        },
    )
    return out_path