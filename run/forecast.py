"""Forward-project a loan or a vintage cohort.

    # backtest a vintage under the macro path that actually occurred
    python -m run.forecast --vintage 2007 --horizon 120

    # project a single loan
    python -m run.forecast --loan F07Q10000001 --horizon 60 --severity 0.5

Backtesting against realised macro is the honest test of the transition models:
it removes scenario error, so any gap between projected and realised curves is
model error.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

import polars as pl  # noqa: E402

from loan_etl.io import scan_dataset  # noqa: E402
from loan_etl.settings import ConfigError, get_settings  # noqa: E402
from loan_model.project import Scenario, project_loan, scenario_from_macro_panel  # noqa: E402
from loan_model.registry import load_all  # noqa: E402
from loan_model.states import load_transition_config  # noqa: E402
from loan_model.train import DEFAULT_MODEL_NAME  # noqa: E402


def _cohort_row(settings, vintage: int, features: list[str]) -> dict:
    """A representative loan for a vintage, covering EVERY model feature.

    Feature-complete by construction rather than by a hand-maintained list: an
    earlier version populated 17 of 52 features and left the rest null, which
    fed the models a loan that does not exist and drove projected defaults to
    zero. Numerics take the median, categoricals the mode.

    This is a cohort-AVERAGE projection, not a portfolio sum. Default is convex
    in the risk drivers, so an average loan understates aggregate losses -- the
    tail is where crisis-vintage defaults actually came from.
    """
    lf = scan_dataset(settings.curated / "loan_month").filter(
        (pl.col("vintage_year") == vintage) & (pl.col("LOAN_AGE") <= 1)
    )
    schema = lf.collect_schema()

    # Resolve the modal state FIRST, then aggregate only within it. Taking a
    # cross-state median while PROPERTY_STATE resolves to the mode produces an
    # incoherent loan: HPI index levels are not comparable across states (CA
    # ~643 in 2007, TX ~224), so a nationwide median HPI_AT_ORIGINATION divided
    # by California's current index yields a mark-to-market LTV that is simply
    # wrong -- it showed the 2007 cohort with large positive equity through the
    # worst house price collapse on record.
    state = (
        lf.select(pl.col("PROPERTY_STATE").drop_nulls().mode().first()).collect().item()
    )
    if state is not None:
        lf = lf.filter(pl.col("PROPERTY_STATE") == state)

    present = [f for f in features if f in schema.names()]
    numeric = [c for c in present if schema[c].is_numeric()]
    other = [c for c in present if c not in numeric]

    exprs = [pl.col(c).median().alias(c) for c in numeric]
    exprs += [pl.col(c).drop_nulls().mode().first().alias(c) for c in other]
    exprs.append(pl.col("MONTHLY_REPORTING_PERIOD").min().alias("MONTHLY_REPORTING_PERIOD"))
    return lf.select(exprs).collect().to_dicts()[0]


def _loan_row(settings, loan_id: str) -> dict:
    lf = scan_dataset(settings.curated / "loan_month").filter(
        pl.col("LOAN_SEQUENCE_NUMBER") == loan_id
    )
    df = lf.sort("MONTHLY_REPORTING_PERIOD").head(1).collect()
    if df.height == 0:
        raise SystemExit(f"Loan {loan_id} not found in the curated panel.")
    return df.to_dicts()[0]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="forecast", description=__doc__)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--vintage", type=int, help="project a vintage cohort")
    g.add_argument("--loan", help="project a single LOAN_SEQUENCE_NUMBER")
    ap.add_argument("--horizon", type=int, default=120)
    ap.add_argument("--severity", type=float, default=0.5)
    ap.add_argument("--start-state", default="CURRENT")
    ap.add_argument("--name", default=DEFAULT_MODEL_NAME)
    ap.add_argument("--out", default=None, help="write the projection to this parquet path")
    args = ap.parse_args(argv)

    try:
        settings = get_settings()
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    cfg = load_transition_config()
    models = load_all(settings, args.name)

    all_features = sorted({f for m in models.values() for f in m.features})
    loan = (
        _cohort_row(settings, args.vintage, all_features)
        if args.vintage
        else _loan_row(settings, args.loan)
    )

    # The scenario must be built for the loan's own state: state-level series
    # (unemployment, HPI) are meaningless once averaged across all 52.
    macro_path = settings.curated / "macro_monthly.parquet"
    scenario = (
        scenario_from_macro_panel(
            macro_path, all_features, state=loan.get("PROPERTY_STATE")
        )
        if macro_path.exists()
        else Scenario(pl.DataFrame({"MONTHLY_REPORTING_PERIOD": []}))
    )
    label = f"vintage {args.vintage}" if args.vintage else args.loan

    unfilled = [f for f in all_features if loan.get(f) is None]
    if unfilled:
        print(f"  WARNING: {len(unfilled)}/{len(all_features)} features unpopulated: {unfilled[:8]}")

    res = project_loan(
        models, cfg, loan, scenario,
        horizon=args.horizon,
        start_state=args.start_state,
        start_period=str(loan.get("MONTHLY_REPORTING_PERIOD")),
        severity=args.severity,
    )

    frame = res.to_frame()
    print(f"\n== projection: {label}, {args.horizon} months ==")
    print(f"  expected loss (severity {args.severity}): {res.expected_loss:,.0f}")
    terminal = {
        s: float(res.cumulative(s)[-1]) for s in cfg.absorbing_states if s in res.space.states
    }
    print("  terminal absorption:")
    for s, v in sorted(terminal.items(), key=lambda kv: -kv[1]):
        if v > 1e-6:
            print(f"    {s:20s} {v:.4%}")
    still_alive = 1.0 - sum(terminal.values())
    print(f"    {'(still transient)':20s} {still_alive:.4%}")

    if args.out:
        frame.write_parquet(args.out)
        print(f"\n  wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
