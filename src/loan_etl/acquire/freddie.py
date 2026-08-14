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

# Freddie has used more than one name for the monthly performance file across
# releases. Current Clarity downloads ship `sample_perf_<year>.txt`; older
# distributions (and the previous loader in this repo) used `sample_svcg_`, and
# the Standard dataset uses `_time_`. All are accepted, in preference order, so
# a naming change is not a hard failure.
ORIG_PATTERNS = ("sample_orig_{year}.txt",)
PERF_PATTERNS = (
    "sample_perf_{year}.txt",
    "sample_svcg_{year}.txt",
    "sample_time_{year}.txt",
)
VINTAGE_DIR_RE = re.compile(r"^sample_(\d{4})$")
VINTAGE_ZIP_RE = re.compile(r"^sample_(\d{4})\.zip$")


def candidate_names(patterns: tuple[str, ...], year: int) -> list[str]:
    return [p.format(year=year) for p in patterns]


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


def _find_member(names: list[str], wanted: list[str] | str) -> str | None:
    """Match a zip member against candidate basenames, in preference order.

    Tolerates nested directories inside the archive and case differences.
    """
    candidates = [wanted] if isinstance(wanted, str) else list(wanted)
    lowered = {Path(n).name.lower(): n for n in names}
    for c in candidates:
        if hit := lowered.get(c.lower()):
            return hit
    return None


def _find_on_disk(directory: Path, candidates: list[str]) -> Path | None:
    for c in candidates:
        p = directory / c
        if p.exists():
            return p
    return None


@contextmanager
def materialize_vintage(raw_dir: Path, year: int, keep_extracted: bool = False) -> Iterator[VintageSource]:
    """Yield the origination and performance text files for one vintage.

    Extracts from the zip if needed, and removes what it extracted on exit
    unless ``keep_extracted`` is set. Files that were already on disk are never
    deleted -- only files this call created.
    """
    orig_names = candidate_names(ORIG_PATTERNS, year)
    perf_names = candidate_names(PERF_PATTERNS, year)

    vdir = raw_dir / f"sample_{year}"
    orig = _find_on_disk(vdir, orig_names)
    perf = _find_on_disk(vdir, perf_names)

    if orig and perf:
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
            resolved: dict[str, Path] = {}
            for kind, wanted, already in (
                ("origination", orig_names, orig),
                ("performance", perf_names, perf),
            ):
                if already is not None:
                    resolved[kind] = already
                    continue
                member = _find_member(names, wanted)
                if member is None:
                    raise AcquisitionError(
                        f"{zip_path.name} contains no {kind} file. Looked for any of "
                        f"{wanted}; archive holds: {[Path(n).name for n in names[:10]]}"
                    )
                target = vdir / Path(member).name
                with zf.open(member) as src, target.open("wb") as dst:
                    while chunk := src.read(1 << 22):
                        dst.write(chunk)
                created.append(target)
                resolved[kind] = target
        yield VintageSource(
            year, resolved["origination"], resolved["performance"], from_zip=True
        )
    finally:
        if not keep_extracted:
            for p in created:
                p.unlink(missing_ok=True)
            if vdir.exists() and not any(vdir.iterdir()):
                vdir.rmdir()


def inspect_raw(raw_dir: Path) -> dict[str, object]:
    """Report what is present under raw/freddie and what the loader makes of it.

    Clarity's download naming is not guaranteed to match what
    ``materialize_vintage`` expects, and a silent "no vintages found" is a
    frustrating first experience. This says exactly what was seen, what was
    recognised, and what to rename.
    """
    if not raw_dir.exists():
        return {"exists": False, "path": str(raw_dir), "recognised": [], "unrecognised": []}

    recognised: list[dict[str, object]] = []
    unrecognised: list[str] = []

    for entry in sorted(raw_dir.iterdir()):
        if entry.name.startswith("."):
            continue
        if m := VINTAGE_ZIP_RE.match(entry.name):
            year = int(m.group(1))
            try:
                with zipfile.ZipFile(entry) as zf:
                    members = [Path(n).name for n in zf.namelist()]
            except zipfile.BadZipFile:
                unrecognised.append(f"{entry.name} (not a valid zip)")
                continue
            orig_hit = _find_member(members, candidate_names(ORIG_PATTERNS, year))
            perf_hit = _find_member(members, candidate_names(PERF_PATTERNS, year))
            recognised.append({
                "vintage": year, "form": "zip", "name": entry.name,
                "has_origination": orig_hit is not None,
                "has_performance": perf_hit is not None,
                "resolved": [orig_hit, perf_hit],
                "members": members[:6],
            })
        elif entry.is_dir() and (m := VINTAGE_DIR_RE.match(entry.name)):
            year = int(m.group(1))
            orig_p = _find_on_disk(entry, candidate_names(ORIG_PATTERNS, year))
            perf_p = _find_on_disk(entry, candidate_names(PERF_PATTERNS, year))
            recognised.append({
                "vintage": year, "form": "directory", "name": entry.name,
                "has_origination": orig_p is not None,
                "has_performance": perf_p is not None,
                "resolved": [orig_p.name if orig_p else None, perf_p.name if perf_p else None],
                "members": sorted(p.name for p in entry.iterdir())[:6],
            })
        else:
            unrecognised.append(entry.name)

    return {
        "exists": True,
        "path": str(raw_dir),
        "recognised": recognised,
        "unrecognised": unrecognised,
        "expected_layout": [
            "sample_<YYYY>.zip  containing an origination and a performance file",
            "sample_<YYYY>/     (already extracted, same two files)",
            f"origination named any of: {list(ORIG_PATTERNS)}",
            f"performance named any of: {list(PERF_PATTERNS)}",
        ],
    }


def source_fingerprint(path: Path) -> dict[str, object]:
    """Checksum + size + line count, recorded in every partition manifest."""
    return {
        "path": str(path),
        "sha256": sha256_file(path),
        "bytes": path.stat().st_size,
        "source_lines": count_lines(path),
    }