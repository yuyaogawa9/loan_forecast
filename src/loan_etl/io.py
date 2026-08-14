"""Data-lake IO: hive-partitioned parquet, manifests, checksums.

Partitioning is by ``vintage_year``, which is the natural unit of the source
files. That choice matters on a disk-constrained machine: a vintage maps 1:1 to
one input file, so ingest needs no shuffle, re-running a single year rewrites
exactly one partition, and the whole stage is idempotent.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 9
ROW_GROUP_SIZE = 250_000


def sha256_file(path: Path, chunk_size: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def git_sha(repo_root: Path) -> str:
    try:
        out = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return out.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# Partition paths
# ---------------------------------------------------------------------------


def partition_dir(base: Path, vintage_year: int) -> Path:
    return base / f"vintage_year={vintage_year}"


def partition_file(base: Path, vintage_year: int) -> Path:
    return partition_dir(base, vintage_year) / "data.parquet"


def sink_partition(lf: pl.LazyFrame, base: Path, vintage_year: int) -> Path:
    """Stream a LazyFrame to one partition file.

    Uses ``sink_parquet`` so the frame is never fully materialised -- the whole
    point on an 8 GB machine. Writes to a temp file and moves into place so an
    interrupted run cannot leave a half-written partition that later looks valid.
    """
    out_dir = partition_dir(base, vintage_year)
    out_dir.mkdir(parents=True, exist_ok=True)
    final = out_dir / "data.parquet"
    tmp = out_dir / "data.parquet.tmp"

    lf.sink_parquet(
        tmp,
        compression=PARQUET_COMPRESSION,
        compression_level=PARQUET_COMPRESSION_LEVEL,
        row_group_size=ROW_GROUP_SIZE,
    )
    tmp.replace(final)
    return final


def scan_dataset(base: Path) -> pl.LazyFrame:
    """Scan a whole hive-partitioned dataset as a single logical table."""
    if not base.exists():
        raise FileNotFoundError(f"No dataset at {base}. Run the ingest stage first.")
    return pl.scan_parquet(base / "**" / "*.parquet", hive_partitioning=True)


def existing_vintages(base: Path) -> list[int]:
    if not base.exists():
        return []
    years = []
    for p in base.glob("vintage_year=*"):
        if (p / "data.parquet").exists():
            try:
                years.append(int(p.name.split("=", 1)[1]))
            except ValueError:
                continue
    return sorted(years)


# ---------------------------------------------------------------------------
# Quarantine
# ---------------------------------------------------------------------------


def write_quarantine(df: pl.DataFrame, base: Path, dataset: str, vintage_year: int) -> Path | None:
    """Persist rejected rows. Nothing is ever dropped silently."""
    if df.height == 0:
        return None
    out_dir = base / dataset / f"vintage_year={vintage_year}"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "rejected.parquet"
    df.write_parquet(path, compression=PARQUET_COMPRESSION)
    return path


# ---------------------------------------------------------------------------
# Manifests
# ---------------------------------------------------------------------------


def write_manifest(manifest_dir: Path, name: str, payload: dict[str, Any]) -> Path:
    manifest_dir.mkdir(parents=True, exist_ok=True)
    path = manifest_dir / f"{name}.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return path


def read_manifest(manifest_dir: Path, name: str) -> dict[str, Any] | None:
    path = manifest_dir / f"{name}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return None


# ---------------------------------------------------------------------------
# Disk safety
# ---------------------------------------------------------------------------


def free_disk_gb(path: Path) -> float:
    target = path
    while not target.exists() and target != target.parent:
        target = target.parent
    usage = shutil.disk_usage(target)
    return usage.free / (1024**3)


def require_free_disk(path: Path, need_gb: float) -> None:
    free = free_disk_gb(path)
    if free < need_gb:
        raise RuntimeError(
            f"Only {free:.1f} GiB free at {path}, need ~{need_gb:.1f} GiB. "
            "Raw text is processed one vintage at a time and deleted after "
            "conversion, but the headroom is still required."
        )