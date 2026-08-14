"""CLI orchestrator for the ETL. Replaces makedb.py.

    python -m run.build_dataset --stage all --vintages 2007
    python -m run.build_dataset --stage ingest --vintages 1999-2024
    python -m run.build_dataset --stage macro
    python -m run.build_dataset --stage all --vintages all --skip-macro

Every stage is idempotent: a vintage whose source checksum already matches its
manifest is skipped unless --force. Raw text is extracted one vintage at a time
and removed after conversion, which is what keeps peak disk within budget.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from loan_etl.acquire.freddie import discover_vintages, inspect_raw  # noqa: E402
from loan_etl.clean.ingest import DATASETS, ingest_vintage  # noqa: E402
from loan_etl.derive.panel import build_loan_month, build_outcomes  # noqa: E402
from loan_etl.io import DataLakeBusy, data_lock, free_disk_gb  # noqa: E402
from loan_etl.settings import ConfigError, get_settings  # noqa: E402
from loan_etl.validate import (  # noqa: E402
    ERROR,
    raise_on_error,
    validate_bronze,
    validate_curated,
)

STAGES = ("inspect", "ingest", "macro", "curate", "validate", "all")


def parse_vintages(spec: str, available: list[int]) -> list[int]:
    if spec == "all":
        if not available:
            raise SystemExit(
                "No vintages found under $DATA_ROOT/raw/freddie. Download the "
                "Sample dataset from Clarity and place sample_YYYY/ or "
                "sample_YYYY.zip there."
            )
        return available
    years: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if "-" in part:
            lo, hi = part.split("-", 1)
            years.update(range(int(lo), int(hi) + 1))
        elif part:
            years.add(int(part))
    return sorted(years)


def _report(results, label: str) -> None:
    for r in results:
        print(f"    {r}")
    errs = [r for r in results if not r.passed and r.severity == ERROR]
    warns = [r for r in results if not r.passed and r.severity != ERROR]
    print(f"    -> {label}: {len(results) - len(errs) - len(warns)} passed, "
          f"{len(warns)} warning(s), {len(errs)} failure(s)")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="build_dataset", description=__doc__)
    ap.add_argument("--stage", choices=STAGES, default="all")
    ap.add_argument("--vintages", default="all", help="e.g. 2007, 1999-2010, or all")
    ap.add_argument("--force", action="store_true", help="re-run even if manifests match")
    ap.add_argument("--skip-macro", action="store_true", help="build without FRED data")
    ap.add_argument(
        "--keep-extracted",
        action="store_true",
        help="keep unzipped text files (uses much more disk)",
    )
    ap.add_argument(
        "--include-pit-unsafe",
        action="store_true",
        help="include retroactively-dated series such as the NBER recession indicator",
    )
    ap.add_argument("--no-fail", action="store_true", help="report gate failures without exiting 1")
    ap.add_argument(
        "--force-unlock",
        action="store_true",
        help="ignore an existing lock (only if you are sure no other build is running)",
    )
    args = ap.parse_args(argv)

    try:
        settings = get_settings()
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    settings.ensure_dirs()

    try:
        with data_lock(settings.data_root, force=args.force_unlock):
            return _run_stages(args, settings)
    except DataLakeBusy as exc:
        print(f"\n{exc}", file=sys.stderr)
        return 3


def _run_stages(args, settings) -> int:
    available = discover_vintages(settings.raw_freddie)
    years = (
        parse_vintages(args.vintages, available)
        if args.stage in ("ingest", "curate", "validate", "all")
        else []
    )

    print(f"DATA_ROOT   : {settings.data_root}")
    print(f"free disk   : {free_disk_gb(settings.data_root):.1f} GiB")
    print(f"schema      : v{settings.schema_version}")
    if years:
        print(f"vintages    : {years[0]}..{years[-1]} ({len(years)})")

    failures = 0

    # --- inspect -----------------------------------------------------------
    if args.stage == "inspect":
        info = inspect_raw(settings.raw_freddie)
        print(f"\n== inspect {info['path']} ==")
        if not info["exists"]:
            print("  directory does not exist yet -- create it and drop the "
                  "Sample dataset downloads in.")
        for r in info["recognised"]:
            ok = "OK " if (r["has_origination"] and r["has_performance"]) else "INCOMPLETE"
            print(f"  [{ok}] {r['name']}  ({r['form']}) "
                  f"orig={r['has_origination']} perf={r['has_performance']}")
            if not (r["has_origination"] and r["has_performance"]):
                print(f"           contains: {r['members']}")
        for u in info["unrecognised"]:
            print(f"  [SKIP] {u}  -- name not recognised")
        if not info["recognised"]:
            print("\n  No usable vintages found. Expected one of:")
            for line in info["expected_layout"]:
                print(f"    {line}")
            print("\n  Download from https://claritydownload.fmapps.freddiemac.com/CRT/#/sflld")
        else:
            years = sorted({r["vintage"] for r in info["recognised"]})
            print(f"\n  {len(years)} vintage(s) ready: {years}")
        return 0

    # --- ingest ------------------------------------------------------------
    if args.stage in ("ingest", "all"):
        print("\n== ingest ==")
        for year in years:
            for dataset in DATASETS:
                m = ingest_vintage(
                    settings, dataset, year,
                    force=args.force, keep_extracted=args.keep_extracted,
                )
                tag = "cached" if m.get("skipped") else "built"
                r = m["rows"]
                print(f"  {year} {dataset:12s} [{tag}] "
                      f"{r['written']:>10,} rows, {r['quarantined']} quarantined")

    # --- macro -------------------------------------------------------------
    if args.stage in ("macro", "all") and not args.skip_macro:
        print("\n== macro ==")
        from loan_etl.acquire.fred import build_macro_panel

        try:
            path = build_macro_panel(settings, force=args.force)
            print(f"  wrote {path}")
        except ConfigError as exc:
            print(f"  SKIPPED: {exc}", file=sys.stderr)
            print("  (re-run with --skip-macro to build without macro data)")
            args.skip_macro = True

    # --- curate ------------------------------------------------------------
    if args.stage in ("curate", "all"):
        print("\n== curate ==")
        for year in years:
            m = build_loan_month(
                settings, year,
                with_macro=not args.skip_macro,
                include_pit_unsafe=args.include_pit_unsafe,
            )
            s = m["summary"]
            print(f"  {year} loan_month  {s['rows']:>10,} rows / {s['loans']:,} loans "
                  f"| defaults {s['default_months']:,} prepay {s['full_prepayments']:,} "
                  f"partial {s['partial_prepayments']:,} chargeoff {s['chargeoffs']:,}")
            o = build_outcomes(settings, year)
            print(f"  {year} outcomes    {o['terminal_outcome_distribution']}")

    # --- validate ----------------------------------------------------------
    if args.stage in ("validate", "all"):
        print("\n== validate ==")
        for year in years:
            print(f"  vintage {year}: bronze")
            bronze = validate_bronze(settings, year)
            _report(bronze, "bronze")
            failures += sum(1 for r in bronze if not r.passed and r.severity == ERROR)

            if (settings.curated / "loan_month").exists():
                print(f"  vintage {year}: curated")
                curated = validate_curated(settings, year)
                _report(curated, "curated")
                failures += sum(1 for r in curated if not r.passed and r.severity == ERROR)

    if failures and not args.no_fail:
        print(f"\n{failures} validation failure(s).", file=sys.stderr)
        return 1
    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())