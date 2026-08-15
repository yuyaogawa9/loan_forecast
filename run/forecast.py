"""Forward-project a loan or a vintage cohort.

    # Monte Carlo: simulate the REAL loans of a vintage under realised macro
    python -m run.forecast --vintage 2007 --method mc --horizon 120

    # matrix projection of one average loan (fast, but see below)
    python -m run.forecast --vintage 2007 --method matrix --horizon 120

    # project a single loan
    python -m run.forecast --loan F07Q10000001 --horizon 60 --severity 0.5

Backtesting against realised macro is the honest test of the transition models:
it removes scenario error, so any gap between projected and realised curves is
model error.

The two methods are not interchangeable. `matrix` advances a distribution over
states, which is exact for a first-order chain but cannot represent duration
dependence, and it projects a single cohort-average loan -- default is convex in
the risk drivers, so the average understates aggregate loss. `mc` samples paths
for the actual loans, carrying each one's own delinquency history, which is what
the path-dependent features need.
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
from loan_model.amounts import load_amount_models  # noqa: E402
from loan_model.lgd import fit_from_panel  # noqa: E402
from loan_model.project import project_loan  # noqa: E402
from loan_model.registry import load_all  # noqa: E402
from loan_model.scenario import MacroPanel, Scenario, scenario_from_macro_panel  # noqa: E402
from loan_model.simulate import simulate  # noqa: E402
from loan_model.states import load_transition_config  # noqa: E402
from loan_model.train import DEFAULT_MODEL_NAME  # noqa: E402
from loan_model.train_amounts import (  # noqa: E402
    CURTAILMENT_TARGET,
    HAZARD_TARGET,
    SEVERITY_TARGET,
)


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


def _cohort_frame(settings, vintage: int, features: list[str], limit: int | None) -> pl.DataFrame:
    """The REAL loans of a vintage, one row each at their first observation.

    The counterpart to `_cohort_row` for simulation. Projecting a single median
    loan is a bias, not just an approximation: default is convex in the risk
    drivers, so an average loan understates aggregate losses -- and the crisis
    vintages' losses came overwhelmingly from the tail.
    """
    lf = scan_dataset(settings.curated / "loan_month").filter(
        pl.col("vintage_year") == vintage
    )
    available = set(lf.collect_schema().names())
    keep = ["LOAN_SEQUENCE_NUMBER", "MONTHLY_REPORTING_PERIOD"]
    keep += [f for f in features if f in available and f not in keep]

    frame = (
        lf.sort("LOAN_SEQUENCE_NUMBER", "MONTHLY_REPORTING_PERIOD")
        .select(keep)
        .unique(subset=["LOAN_SEQUENCE_NUMBER"], keep="first")
        .collect(engine="streaming")
        .sort("LOAN_SEQUENCE_NUMBER")
    )
    return frame.head(limit) if limit else frame


def _loan_row(settings, loan_id: str) -> dict:
    lf = scan_dataset(settings.curated / "loan_month").filter(
        pl.col("LOAN_SEQUENCE_NUMBER") == loan_id
    )
    df = lf.sort("MONTHLY_REPORTING_PERIOD").head(1).collect()
    if df.height == 0:
        raise SystemExit(f"Loan {loan_id} not found in the curated panel.")
    return df.to_dicts()[0]


def _amount_models(settings, args):
    """Resolve severity plus the optional curtailment pair.

    Preference order: the fitted quantile models, then the bucketed empirical
    table, then a flat scalar. Explicit rather than silent, because the three
    give materially different loss distributions.
    """
    fitted = {} if args.no_amounts else load_amount_models(settings, args.name)
    severity = fitted.get(SEVERITY_TARGET)
    hazard = fitted.get(HAZARD_TARGET)
    curtail = fitted.get(CURTAILMENT_TARGET)

    if severity is not None:
        print(f"  severity: fitted quantile model, {len(severity.quantiles)} quantiles "
              f"conditioned on outcome ({severity.metrics.get('train_rows', 0):,} events)")
    elif args.empirical_severity:
        severity = fit_from_panel(scan_dataset(settings.curated / "loan_month"))
        print(f"  severity: empirical buckets, {severity.meta['n_observations']:,} disposed "
              f"loans, pooled median {severity.meta['pooled_median']:.1%}")
    else:
        severity = args.severity
        print(f"  severity: flat {args.severity:.1%}")

    if hazard is not None and curtail is not None:
        print(f"  curtailment: hazard {hazard.metrics.get('train_rate', 0):.1%} monthly "
              "+ fitted amount quantiles")
    else:
        print("  curtailment: NOT modelled -- balances follow the scheduled path")
    return severity, hazard, curtail


def _run_monte_carlo(settings, cfg, models, all_features, args) -> int:
    if not args.vintage:
        print("--method mc currently simulates a vintage cohort; use --vintage.", file=sys.stderr)
        return 2

    unsafe = [fs for fs, m in models.items() if m.markov_safe]
    if unsafe:
        print(
            f"  NOTE: models {unsafe} were fitted markov-safe, so the duration "
            "features that motivate simulation are absent. Retrain with "
            "--allow-path-dependent to get the benefit."
        )

    severity, hazard, curtail = _amount_models(settings, args)
    # The amount models carry their own feature lists; the cohort frame has to
    # satisfy every model that will score it, not just the transition ones.
    needed = set(all_features) | {"PRIOR_UPB"}
    for extra in (severity, hazard, curtail):
        if hasattr(extra, "features"):
            needed |= set(extra.features)

    loans = _cohort_frame(settings, args.vintage, sorted(needed), args.paths)
    if loans.height == 0:
        print(f"No loans found for vintage {args.vintage}.", file=sys.stderr)
        return 1

    macro_path = settings.curated / "macro_monthly.parquet"
    macro = MacroPanel.load(macro_path) if macro_path.exists() else None
    start = str(loans["MONTHLY_REPORTING_PERIOD"].min())

    print(f"\n== monte carlo: vintage {args.vintage} ==")
    print(f"  loans {loans.height:,}  horizon {args.horizon}  start {start}")

    result = simulate(
        models, cfg, loans, macro,
        horizon=args.horizon,
        seed=args.seed,
        replicates=args.replicates,
        chunk_paths=args.chunk_paths,
        severity=severity,
        prepay_hazard=hazard,
        curtailment=curtail,
        start_period=start,
        start_state=args.start_state,
        collect_paths=bool(args.export_paths),
    )

    summary = result.by_period()
    original = float(loans["ORIGINAL_UPB"].sum()) if "ORIGINAL_UPB" in loans.columns else 0.0
    final = summary.tail(1).to_dicts()[0] if summary.height else {}
    events = int(final.get("CUM_CREDIT_EVENTS", 0))
    loss = float(final.get("CUM_LOSS", 0.0))

    print(f"  paths simulated       : {result.n_paths:,}")
    print(f"  cumulative credit events: {events:,} ({events / max(result.n_paths, 1):.2%})")
    print(f"  cumulative loss       : {loss:,.0f}", end="")
    print(f"  ({loss / original:.2%} of original UPB)" if original else "")
    if summary.height:
        peak = summary.sort("DELINQUENCY_RATE", descending=True).head(1).to_dicts()[0]
        print(f"  peak delinquency      : {peak['DELINQUENCY_RATE']:.2%} in {peak['PERIOD']}")

    if args.out:
        summary.write_parquet(args.out)
        print(f"\n  wrote {args.out}")
    if args.export_paths and result.paths is not None:
        result.paths.write_parquet(args.export_paths)
        print(f"  wrote {result.paths.height:,} path rows to {args.export_paths}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="forecast", description=__doc__)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--vintage", type=int, help="project a vintage cohort")
    g.add_argument("--loan", help="project a single LOAN_SEQUENCE_NUMBER")
    ap.add_argument("--method", choices=("matrix", "mc"), default="matrix")
    ap.add_argument("--horizon", type=int, default=120)
    ap.add_argument("--severity", type=float, default=0.5)
    ap.add_argument(
        "--empirical-severity",
        action="store_true",
        help="draw LGD from realised LOSS_SEVERITY instead of the flat --severity",
    )
    ap.add_argument(
        "--no-amounts",
        action="store_true",
        help="ignore fitted amount models; use flat/empirical severity and scheduled balances",
    )
    ap.add_argument("--start-state", default="CURRENT")
    ap.add_argument("--name", default=DEFAULT_MODEL_NAME)
    ap.add_argument("--paths", type=int, default=None, help="mc: cap the loans simulated")
    ap.add_argument("--replicates", type=int, default=None, help="mc: draws per loan")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--chunk-paths", type=int, default=None)
    ap.add_argument(
        "--export-paths", default=None, help="mc: write per-loan paths to this parquet"
    )
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

    if args.method == "mc":
        return _run_monte_carlo(settings, cfg, models, all_features, args)
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
