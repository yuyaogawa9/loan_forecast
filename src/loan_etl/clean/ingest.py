"""Raw pipe-delimited text -> typed, sentinel-corrected bronze parquet.

Replaces the pure-Python per-line ``split('|')`` loop that built an all-``object``
pandas DataFrame. The whole read is now a single lazy ``scan_csv`` streamed
straight to ``sink_parquet``, so a vintage is never fully resident in memory.

Two passes are made over each source file, deliberately:

  pass 1 -- profile: row counts, per-column null counts, range/enum violation
           counts, and collection of the (normally tiny) set of rows that fail
           structural checks and must be quarantined.
  pass 2 -- sink: cast and stream the surviving rows to parquet.

The alternative, a single pass, cannot both aggregate statistics and write rows
from one lazy plan. The profile pass is what makes silent corruption visible --
it is the reason a spike in range violations surfaces an undocumented sentinel
in a new release rather than quietly nulling a column.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from ..acquire.freddie import materialize_vintage, source_fingerprint
from ..io import (
    partition_file,
    read_manifest,
    sink_partition,
    utc_now,
    write_manifest,
    write_quarantine,
)
from ..schema import TableSchema, check_arity, load_schema
from ..settings import REPO_ROOT, Settings

DATASETS = ("origination", "performance")


class IngestError(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# Expressions
# ---------------------------------------------------------------------------


def _scan_raw(path: Path, schema: TableSchema) -> pl.LazyFrame:
    """Scan with an all-Utf8 schema.

    Reading every field as text first is what preserves NET_SALE_PROCEEDS' "C"
    and "U" codes and lets sentinels be nulled *before* casting. Letting the CSV
    reader infer or cast would destroy both.

    ``quote_char=None`` because the files are unquoted and SELLER_NAME can
    legitimately contain a double-quote, which would otherwise swallow the rest
    of the record.
    """
    return pl.scan_csv(
        path,
        separator=schema.separator,
        has_header=schema.has_header,
        schema=schema.read_schema(),
        quote_char=None,
        truncate_ragged_lines=False,
        encoding="utf8-lossy",
        infer_schema_length=0,
    )


def _reject_predicate(schema: TableSchema) -> pl.Expr | None:
    """Structural failures that make a row unusable: missing key, malformed id."""
    preds: list[pl.Expr] = []
    for f in schema.fields:
        col = pl.col(f.name).str.strip_chars()
        if f.primary_key or f.foreign_key:
            preds.append(col.is_null() | (col.str.len_chars() == 0))
        if f.pattern:
            preds.append(col.is_not_null() & ~col.str.contains(f.pattern))
    if not preds:
        return None
    out = preds[0]
    for p in preds[1:]:
        out = out | p
    return out


def _vintage_exprs(schema: TableSchema) -> list[pl.Expr]:
    """Vintage year/quarter decoded from the loan id (F07Q1... -> 2007, Q1).

    Kept as real columns so validate.py can cross-check them against the source
    filename -- a mislabelled or misplaced input file is otherwise invisible.
    """
    key = schema.primary_key or "LOAN_SEQUENCE_NUMBER"
    if schema.get(key) is None:
        return []
    yy = pl.col(key).str.slice(1, 2).cast(pl.Int32, strict=False)
    return [
        pl.when(yy >= 90).then(1900 + yy).otherwise(2000 + yy).alias("LOAN_VINTAGE_YEAR"),
        pl.col(key).str.slice(4, 1).cast(pl.Int8, strict=False).alias("LOAN_VINTAGE_QUARTER"),
    ]


def _profile_exprs(schema: TableSchema) -> list[pl.Expr]:
    exprs: list[pl.Expr] = [pl.len().alias("n_rows")]
    exprs += schema.range_violation_exprs()
    exprs += schema.enum_violation_exprs()
    for f in schema.fields:
        col = pl.col(f.name).str.strip_chars()
        if f.nulls:
            col = pl.when(col.is_in(list(f.nulls))).then(None).otherwise(col)
        exprs.append(col.is_null().sum().alias(f"nulls__{f.name}"))
    return exprs


# ---------------------------------------------------------------------------
# Stages
# ---------------------------------------------------------------------------


def profile_source(path: Path, schema: TableSchema) -> tuple[dict[str, Any], pl.DataFrame]:
    """Pass 1. Returns (profile stats, rows to quarantine)."""
    lf = _scan_raw(path, schema)
    stats = lf.select(_profile_exprs(schema)).collect(engine="streaming").to_dicts()[0]

    reject = _reject_predicate(schema)
    if reject is None:
        bad = pl.DataFrame()
    else:
        bad = lf.filter(reject).collect(engine="streaming")
    return stats, bad


def build_bronze(path: Path, schema: TableSchema, out_base: Path, year: int) -> Path:
    """Pass 2. Cast and stream to the vintage partition."""
    lf = _scan_raw(path, schema)

    reject = _reject_predicate(schema)
    if reject is not None:
        lf = lf.filter(~reject)

    # Cast first so derived expressions see typed columns where they need them,
    # and the YYYYMM string columns while they are still strings.
    lf = lf.with_columns(schema.derived_exprs() + _vintage_exprs(schema))
    lf = lf.with_columns(schema.cast_exprs())

    # Sorting inside the partition buys real compression on the repeated loan id
    # and makes the downstream loan-level window functions cheap.
    sort_keys = [k for k in ("LOAN_SEQUENCE_NUMBER", "MONTHLY_REPORTING_PERIOD") if schema.get(k)]
    if sort_keys:
        lf = lf.sort(sort_keys)

    return sink_partition(lf, out_base, year)


def ingest_vintage(
    settings: Settings,
    dataset: str,
    year: int,
    *,
    force: bool = False,
    keep_extracted: bool = False,
) -> dict[str, Any]:
    """Ingest one dataset for one vintage year. Idempotent."""
    if dataset not in DATASETS:
        raise IngestError(f"dataset must be one of {DATASETS}, got {dataset!r}")

    schema = load_schema(dataset, settings.schema_version, settings.schema_dir)
    out_base = settings.bronze / dataset
    manifest_name = f"bronze__{dataset}__{year}"

    with materialize_vintage(settings.raw_freddie, year, keep_extracted) as src:
        path = src.path_for(dataset)

        # Loud failure on layout mismatch. The previous loader compared field
        # counts per row and silently discarded every non-matching line, which
        # meant a layout change wiped a whole vintage with no error.
        check_arity(path, schema)

        fingerprint = source_fingerprint(path)

        prior = read_manifest(settings.manifests, manifest_name)
        if (
            not force
            and prior
            and prior.get("source", {}).get("sha256") == fingerprint["sha256"]
            and partition_file(out_base, year).exists()
        ):
            prior["skipped"] = True
            return prior

        stats, bad_rows = profile_source(path, schema)
        quarantine_path = write_quarantine(bad_rows, settings.quarantine, dataset, year)
        out_path = build_bronze(path, schema, out_base, year)

    output_rows = (
        pl.scan_parquet(out_path).select(pl.len()).collect().item()
    )
    source_rows = int(stats.pop("n_rows"))
    quarantined = int(bad_rows.height)

    manifest: dict[str, Any] = {
        "dataset": dataset,
        "vintage_year": year,
        "schema_version": schema.version,
        "schema_file": str(schema.source_path),
        "etl_version": __import__("loan_etl").__version__,
        "git_sha": _git_sha(),
        "created_utc": utc_now(),
        "source": fingerprint,
        "rows": {
            "source_lines": fingerprint["source_lines"],
            "parsed": source_rows,
            "quarantined": quarantined,
            "written": int(output_rows),
        },
        "quarantine_path": str(quarantine_path) if quarantine_path else None,
        "output_path": str(out_path),
        "output_bytes": out_path.stat().st_size,
        "profile": {k: int(v) for k, v in stats.items() if v is not None},
        "skipped": False,
    }
    write_manifest(settings.manifests, manifest_name, manifest)
    return manifest


def _git_sha() -> str:
    from ..io import git_sha

    return git_sha(REPO_ROOT)