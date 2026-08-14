"""Locate, verify and materialise Freddie Mac source files.

Freddie's Clarity portal (claritydownload.fmapps.freddiemac.com) is a
session-authenticated web UI with no public bulk-download API, so acquisition is
manual by necessity. What this module owns instead is the *registry*: what you
dropped in, its checksum, its line count, and whether it has already been
ingested. That is what makes re-runs idempotent and the output auditable.

Expected raw layout under ``$DATA_ROOT/raw/freddie`` -- either form works:

    sample_2007/sample_orig_2007.txt    # already extracted
    sample_2007/sample_svcg_2007.txt
    sample_2007.zip                     # or still zipped

Zips are extracted one vintage at a time and the extracted text is deleted after
conversion. With ~12 GB of raw text for the full Sample dataset against 22 GiB
free, keeping every vintage extracted at once is not an option.
"""

from __future__ import annotations

import re
import zipfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from ..io import sha256_file

ORIG_PATTERN = "sample_orig_{year}.txt"
PERF_PATTERN = "sample_svcg_{year}.txt"
VINTAGE_DIR_RE = re.compile(r"^sample_(\d{4})$")
VINTAGE_ZIP_RE = re.compile(r"^sample_(\d{4})\.zip$")


class AcquisitionError(RuntimeError):
    pass


@dataclass(frozen=True)
class VintageSource:
    year: int
    orig: Path
    perf: Path
    from_zip: bool

    def path_for(self, dataset: str) -> Path:
        return self.orig if dataset == "origination" else self.perf


def count_lines(path: Path, chunk_size: int = 1 << 22) -> int:
    """Fast newline count. Counts a trailing unterminated line."""
    total = 0
    last_byte = b"\n"
    with path.open("rb") as fh:
        while chunk := fh.read(chunk_size):
            total += chunk.count(b"\n")
            last_byte = chunk[-1:]
    if last_byte not in (b"\n", b""):
        total += 1
    return total


def discover_vintages(raw_dir: Path) -> list[int]:
    """Vintage years available as either an extracted directory or a zip."""
    if not raw_dir.exists():
        return []
    years: set[int] = set()
    for entry in raw_dir.iterdir():
        if entry.is_dir() and (m := VINTAGE_DIR_RE.match(entry.name)):
            years.add(int(m.group(1)))
        elif entry.is_file() and (m := VINTAGE_ZIP_RE.match(entry.name)):
            years.add(int(m.group(1)))
    return sorted(years)


def _find_member(names: list[str], wanted: str) -> str | None:
    """Match a zip member by basename, tolerating nested directories."""
    for n in names:
        if Path(n).name.lower() == wanted.lower():
            return n
    return None


@contextmanager
def materialize_vintage(raw_dir: Path, year: int, keep_extracted: bool = False) -> Iterator[VintageSource]:
    """Yield the origination and performance text files for one vintage.

    Extracts from the zip if needed, and removes what it extracted on exit
    unless ``keep_extracted`` is set. Files that were already on disk are never
    deleted -- only files this call created.
    """
    orig_name = ORIG_PATTERN.format(year=year)
    perf_name = PERF_PATTERN.format(year=year)

    vdir = raw_dir / f"sample_{year}"
    orig = vdir / orig_name
    perf = vdir / perf_name

    if orig.exists() and perf.exists():
        yield VintageSource(year, orig, perf, from_zip=False)
        return

    zip_path = raw_dir / f"sample_{year}.zip"
    if not zip_path.exists():
        raise AcquisitionError(
            f"Vintage {year}: no extracted files at {vdir} and no {zip_path.name}. "
            f"Download the Sample dataset from Clarity and place it under {raw_dir}."
        )

    vdir.mkdir(parents=True, exist_ok=True)
    created: list[Path] = []
    try:
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
            for target, wanted in ((orig, orig_name), (perf, perf_name)):
                if target.exists():
                    continue
                member = _find_member(names, wanted)
                if member is None:
                    raise AcquisitionError(
                        f"{zip_path.name} does not contain {wanted}. Members: {names[:10]}"
                    )
                with zf.open(member) as src, target.open("wb") as dst:
                    while chunk := src.read(1 << 22):
                        dst.write(chunk)
                created.append(target)
        yield VintageSource(year, orig, perf, from_zip=True)
    finally:
        if not keep_extracted:
            for p in created:
                p.unlink(missing_ok=True)
            if vdir.exists() and not any(vdir.iterdir()):
                vdir.rmdir()


def source_fingerprint(path: Path) -> dict[str, object]:
    """Checksum + size + line count, recorded in every partition manifest."""
    return {
        "path": str(path),
        "sha256": sha256_file(path),
        "bytes": path.stat().st_size,
        "source_lines": count_lines(path),
    }