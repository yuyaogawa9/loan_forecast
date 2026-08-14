"""CLI for the multi-state transition models.

    python -m run.train_models --stage all
    python -m run.train_models --stage sample --force
    python -m run.train_models --stage train --only 00,01
    python -m run.train_models --stage evaluate

Stages are idempotent: the training sample is rebuilt only with --force.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

import polars as pl  # noqa: E402

from loan_etl.io import DataLakeBusy, data_lock  # noqa: E402
from loan_etl.settings import ConfigError, get_settings  # noqa: E402
from loan_model.evaluate import evaluate_all  # noqa: E402
from loan_model.registry import load_all  # noqa: E402
from loan_model.sampling import build_training_sample, verify_reweighting  # noqa: E402
from loan_model.states import load_transition_config, verify_against_data  # noqa: E402
from loan_model.train import DEFAULT_MODEL_NAME, train_all  # noqa: E402

STAGES = ("sample", "train", "evaluate", "verify", "all")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="train_models", description=__doc__)
    ap.add_argument("--stage", choices=STAGES, default="all")
    ap.add_argument("--name", default=DEFAULT_MODEL_NAME)
    ap.add_argument("--only", default=None, help="comma-separated from-states")
    ap.add_argument("--force", action="store_true", help="rebuild the training sample")
    ap.add_argument(
        "--allow-path-dependent",
        action="store_true",
        help="fit with path-dependent features (scoring only -- NOT projectable)",
    )
    ap.add_argument("--eval-years", default="2022,2023")
    ap.add_argument("--force-unlock", action="store_true")
    args = ap.parse_args(argv)

    try:
        settings = get_settings()
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    settings.ensure_dirs()
    cfg = load_transition_config()
    only = args.only.split(",") if args.only else None

    print(f"DATA_ROOT   : {settings.data_root}")
    print(f"transitions : v{cfg.version}  from-states={list(cfg.from_states)}")
    print(f"features    : {'markov-safe' if not args.allow_path_dependent else 'ALL (not projectable)'}")

    try:
        with data_lock(settings.data_root, force=args.force_unlock):
            return _run(args, settings, cfg, only)
    except DataLakeBusy as exc:
        print(f"\n{exc}", file=sys.stderr)
        return 3


def _run(args, settings, cfg, only) -> int:
    if args.stage in ("sample", "all"):
        print("\n== sample ==")
        m = build_training_sample(settings, cfg, force=args.force)
        if m.get("skipped"):
            print(f"  cached: {m['rows']:,} rows across {m['partitions']} partitions")
        else:
            print(
                f"  population {m['population_rows']:,} -> sampled {m['sampled_rows']:,} "
                f"({m['reduction_factor']}x reduction)"
            )
            for k, v in m["rows_by_from_state"].items():
                print(f"    {k:8s} {v:>9,}")

    if args.stage in ("train", "all"):
        print("\n== train ==")
        train_all(
            settings, cfg,
            name=args.name,
            markov_safe=not args.allow_path_dependent,
            only=only,
        )

    if args.stage in ("evaluate", "all"):
        print("\n== evaluate (UNSAMPLED holdout) ==")
        years = [int(y) for y in args.eval_years.split(",")]
        models = load_all(settings, args.name)
        if only:
            models = {k: v for k, v in models.items() if k in only}
        table, scores = evaluate_all(settings, models, cfg, years=years)
        for s in scores:
            if s.get("n"):
                print(f"  {s['from_state']:8s} n={s['n']:>9,} logloss={s['multi_logloss']:.5f}")
                print(f"           AUC {s['auc_by_destination']}")
        if table.height:
            worst = table.sort("abs_error", descending=True).head(8)
            print("\n  calibration, worst absolute errors:")
            with pl.Config(tbl_rows=10, tbl_width_chars=140):
                print(worst.select(
                    "from_state", "destination", "actual_rate", "predicted_rate",
                    "abs_error", "actual_count",
                ))

    if args.stage in ("verify", "all"):
        print("\n== verify ==")
        rw = verify_reweighting(settings, cfg)
        bad = rw.filter(~pl.col("ok"))
        print(f"  re-weighting: {rw.height - bad.height}/{rw.height} strata within tolerance")
        if bad.height:
            with pl.Config(tbl_rows=10, tbl_width_chars=140):
                print(bad.select("FROM_STATE", "EVENT", "n_population", "n_reweighted", "rel_error"))
        drift = verify_against_data(
            pl.scan_parquet(settings.curated / "loan_month" / "**" / "*.parquet"), cfg
        )
        moved = drift.filter(pl.col("n_drift") != 0)
        print(f"  config drift: {moved.height} from-state(s) differ from the pinned counts")
        if moved.height:
            with pl.Config(tbl_rows=10, tbl_width_chars=140):
                print(moved)

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
