"""Data-lake IO: hive-partitioned parquet, manifests, checksums.

Partitioning is by ``vintage_year``, which is the natural unit of the source
files. That choice matters on a disk-constrained machine: a vintage maps 1:1 to
one input file, so ingest needs no shuffle, re-running a single year rewrites
exactly one partition, and the whole stage is idempotent.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

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
    # The temp name carries the PID. A shared name means two processes writing
    # the same partition race: one renames the temp away and the other's
    # os.replace fails with a confusing FileNotFoundError. data_lock() should
    # prevent concurrent runs, but the unique name makes the write safe anyway.
    tmp = out_dir / f"data.parquet.{os.getpid()}.tmp"

    try:
        lf.sink_parquet(
            tmp,
            compression=PARQUET_COMPRESSION,
            compression_level=PARQUET_COMPRESSION_LEVEL,
            row_group_size=ROW_GROUP_SIZE,
        )
        if not tmp.exists():
            raise RuntimeError(
                f"sink_parquet reported success but {tmp} is missing. "
                "Another process may be writing the same data lake."
            )
        tmp.replace(final)
    except BaseException:
        tmp.unlink(missing_ok=True)   # never leave a partial temp behind
        raise
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


class DataLakeBusy(RuntimeError):
    pass


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True   # exists, owned by someone else
    return True


@contextmanager
def data_lock(data_root: Path, *, force: bool = False) -> Iterator[Path]:
    """Advisory single-writer lock over a data lake.

    Two concurrent builds against one DATA_ROOT interleave partition writes and
    corrupt each other's temp files. This makes the second one fail immediately
    with an explanation instead of halfway through with a stray
    FileNotFoundError.

    A lock left by a dead process is stale and gets taken over automatically, so
    a crashed run does not require manual cleanup.
    """
    data_root.mkdir(parents=True, exist_ok=True)
    lock = data_root / ".etl.lock"

    if lock.exists() and not force:
        try:
            info = json.loads(lock.read_text())
        except (json.JSONDecodeError, OSError):
            info = {}
        pid = info.get("pid")
        if isinstance(pid, int) and pid != os.getpid() and _pid_alive(pid):
            raise DataLakeBusy(
                f"Another build is already running against {data_root} "
                f"(pid {pid}, started {info.get('started', 'unknown')}).\n"
                "Wait for it to finish, stop it, or pass --force-unlock if you "
                "are certain it is dead."
            )

    lock.write_text(json.dumps({"pid": os.getpid(), "started": utc_now()}, indent=2))
    try:
        yield lock
    finally:
        try:
            if json.loads(lock.read_text()).get("pid") == os.getpid():
                lock.unlink(missing_ok=True)
        except (json.JSONDecodeError, OSError):
            lock.unlink(missing_ok=True)


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