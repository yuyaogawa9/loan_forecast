"""Validation gates.

Every gate returns a result rather than raising immediately, so a run reports
*all* problems at once instead of stopping at the first. ``raise_on_error``
turns accumulated errors into a failed build.

The gates exist because the previous pipeline's failure modes were all silent:
rows vanished, sentinels became features, and the perf/orig join key was NaN.
Anything that could fail quietly gets a gate here.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from typing import Any, Iterable

import polars as pl

from .io import read_manifest, scan_dataset
from .settings import Settings

ERROR = "error"
WARNING = "warning"


class ValidationFailed(RuntimeError):
    pass


@dataclass
class GateResult:
    name: str
    passed: bool
    severity: str = ERROR
    detail: str = ""
    metrics: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        mark = "PASS" if self.passed else ("FAIL" if self.severity == ERROR else "WARN")
        return f"[{mark}] {self.name}" + (f" -- {self.detail}" if self.detail else "")


def raise_on_error(results: Iterable[GateResult]) -> list[GateResult]:
    results = list(results)
    errors = [r for r in results if not r.passed and r.severity == ERROR]
    if errors:
        lines = "\n".join(f"  - {r.name}: {r.detail}" for r in errors)
        raise ValidationFailed(f"{len(errors)} validation gate(s) failed:\n{lines}")
    return results


# ---------------------------------------------------------------------------
# Bronze gates
# ---------------------------------------------------------------------------


def gate_row_accounting(settings: Settings, dataset: str, year: int) -> GateResult:
    """Every source line is accounted for: written + quarantined == parsed."""
    m = read_manifest(settings.manifests, f"bronze__{dataset}__{year}")
    if not m:
        return GateResult("row_accounting", False, ERROR, f"no manifest for {dataset} {year}")
    r = m["rows"]
    ok = r["written"] + r["quarantined"] == r["parsed"] == r["source_lines"]
    return GateResult(
        "row_accounting",
        ok,
        ERROR,
        "" if ok else f"source={r['source_lines']} parsed={r['parsed']} "
                      f"written={r['written']} quarantined={r['quarantined']}",
        metrics=r,
    )


def gate_primary_key_unique(settings: Settings, year: int) -> GateResult:
    lf = scan_dataset(settings.bronze / "origination").filter(pl.col("vintage_year") == year)
    stats = lf.select(
        pl.len().alias("n"),
        pl.col("LOAN_SEQUENCE_NUMBER").n_unique().alias("n_unique"),
        pl.col("LOAN_SEQUENCE_NUMBER").null_count().alias("n_null"),
    ).collect().to_dicts()[0]
    ok = stats["n"] == stats["n_unique"] and stats["n_null"] == 0
    return GateResult(
        "origination_pk_unique",
        ok,
        ERROR,
        "" if ok else f"{stats['n']} rows, {stats['n_unique']} unique, {stats['n_null']} null",
        metrics=stats,
    )


def gate_referential_integrity(settings: Settings, year: int) -> GateResult:
    """Every performance loan must exist in origination.

    Under the old loader this was guaranteed to fail: the off-by-one coercion
    turned LOAN_SEQUENCE_NUMBER into NaN on the origination side.
    """
    orig = (
        scan_dataset(settings.bronze / "origination")
        .filter(pl.col("vintage_year") == year)
        .select("LOAN_SEQUENCE_NUMBER")
    )
    perf = (
        scan_dataset(settings.bronze / "performance")
        .filter(pl.col("vintage_year") == year)
        .select("LOAN_SEQUENCE_NUMBER")
        .unique()
    )
    orphans = perf.join(orig.unique(), on="LOAN_SEQUENCE_NUMBER", how="anti").collect()
    n = orphans.height
    ok = n == 0
    sample = orphans["LOAN_SEQUENCE_NUMBER"].head(5).to_list()
    return GateResult(
        "referential_integrity",
        ok,
        ERROR,
        "" if ok else f"{n} performance loan(s) absent from origination, e.g. {sample}",
        metrics={"orphan_loans": n},
    )


def gate_vintage_consistency(settings: Settings, dataset: str, year: int) -> GateResult:
    """Vintage encoded in the loan id must match the partition it landed in."""
    lf = scan_dataset(settings.bronze / dataset).filter(pl.col("vintage_year") == year)
    n_bad = lf.filter(pl.col("LOAN_VINTAGE_YEAR") != year).select(pl.len()).collect().item()
    ok = n_bad == 0
    return GateResult(
        f"vintage_consistency__{dataset}",
        ok,
        ERROR,
        "" if ok else f"{n_bad} rows whose loan id vintage != partition {year}",
        metrics={"mismatched_rows": n_bad},
    )


def gate_loan_age_monotonic(settings: Settings, year: int) -> GateResult:
    lf = scan_dataset(settings.bronze / "performance").filter(pl.col("vintage_year") == year)
    n_bad = (
        lf.sort(["LOAN_SEQUENCE_NUMBER", "MONTHLY_REPORTING_PERIOD"])
        .with_columns(
            pl.col("LOAN_AGE").diff().over("LOAN_SEQUENCE_NUMBER").alias("_d_age"),
            pl.col("MONTHLY_REPORTING_PERIOD")
            .cast(pl.Int32, strict=False)
            .diff()
            .over("LOAN_SEQUENCE_NUMBER")
            .alias("_d_period"),
        )
        .filter((pl.col("_d_age") < 0) | (pl.col("_d_period") <= 0))
        .select(pl.len())
        .collect()
        .item()
    )
    ok = n_bad == 0
    return GateResult(
        "loan_age_monotonic",
        ok,
        ERROR,
        "" if ok else f"{n_bad} row(s) with decreasing LOAN_AGE or non-increasing period",
        metrics={"bad_rows": n_bad},
    )


def gate_terminal_event_once(settings: Settings, year: int) -> GateResult:
    """A zero-balance code may occur at most once per loan, on its last record."""
    lf = scan_dataset(settings.bronze / "performance").filter(pl.col("vintage_year") == year)
    per_loan = (
        lf.group_by("LOAN_SEQUENCE_NUMBER")
        .agg(
            pl.col("ZERO_BALANCE_CODE").is_not_null().sum().alias("n_zb"),
            pl.col("MONTHLY_REPORTING_PERIOD").max().alias("last_period"),
            pl.col("MONTHLY_REPORTING_PERIOD")
            .filter(pl.col("ZERO_BALANCE_CODE").is_not_null())
            .max()
            .alias("zb_period"),
        )
        .collect()
    )
    multi = per_loan.filter(pl.col("n_zb") > 1)
    not_last = per_loan.filter(
        pl.col("zb_period").is_not_null() & (pl.col("zb_period") != pl.col("last_period"))
    )
    ok = multi.height == 0 and not_last.height == 0
    return GateResult(
        "terminal_event_once",
        ok,
        ERROR,
        "" if ok else f"{multi.height} loan(s) with >1 zero-balance code, "
                      f"{not_last.height} where it is not the final record",
        metrics={"multi_zb": multi.height, "zb_not_last": not_last.height},
    )


def gate_no_sentinel_leakage(settings: Settings, year: int) -> GateResult:
    """The headline check: no sentinel survived into a numeric feature.

    Under the previous loader CREDIT_SCORE.max() was 9999. This is the clearest
    single before/after signal that the cleaning stage is doing its job.
    """
    lf = scan_dataset(settings.bronze / "origination").filter(pl.col("vintage_year") == year)
    stats = lf.select(
        pl.col("CREDIT_SCORE").max().alias("credit_score_max"),
        pl.col("ORIGINAL_DEBT_TO_INCOME").max().alias("dti_max"),
        pl.col("ORIGINAL_LOAN_TO_VALUE").max().alias("ltv_max"),
        pl.col("ORIGINAL_COMBINED_LOAN_TO_VALUE").max().alias("cltv_max"),
    ).collect().to_dicts()[0]

    problems = []
    if (v := stats["credit_score_max"]) is not None and v > 850:
        problems.append(f"CREDIT_SCORE max={v} (>850)")
    if (v := stats["dti_max"]) is not None and v > 65:
        problems.append(f"DTI max={v} (>65)")
    for key, label in (("ltv_max", "LTV"), ("cltv_max", "CLTV")):
        if (v := stats[key]) is not None and v >= 999:
            problems.append(f"{label} max={v} (>=999)")

    return GateResult(
        "no_sentinel_leakage",
        not problems,
        ERROR,
        "; ".join(problems),
        metrics=stats,
    )


def gate_range_and_enum_violations(settings: Settings, dataset: str, year: int) -> GateResult:
    """Values nulled by a range check rather than a declared sentinel.

    Non-zero is not automatically wrong, but it means the source contains values
    the schema does not explain -- typically an undocumented sentinel in a new
    release. Warning-level so a new release does not hard-block the build.
    """
    m = read_manifest(settings.manifests, f"bronze__{dataset}__{year}")
    if not m:
        return GateResult(f"range_enum__{dataset}", False, ERROR, "no manifest")
    hits = {
        k: v
        for k, v in m["profile"].items()
        if k.startswith(("range_violations__", "enum_violations__")) and v
    }
    return GateResult(
        f"range_enum__{dataset}",
        not hits,
        WARNING,
        "" if not hits else f"unexplained values: {hits}",
        metrics=hits,
    )


def gate_null_rate_drift(
    settings: Settings, dataset: str, year: int, tolerance: float = 0.25
) -> GateResult:
    """Compare this vintage's null rates against the median of other vintages.

    Cross-vintage rather than run-over-run: a column that is 2% null everywhere
    else and 90% null in one vintage is the signal worth catching, and it does
    not require retaining run history.
    """
    others: dict[str, list[float]] = {}
    this: dict[str, float] = {}

    for name in sorted(settings.manifests.glob(f"bronze__{dataset}__*.json")):
        m = read_manifest(settings.manifests, name.stem)
        if not m or not m["rows"]["written"]:
            continue
        n = m["rows"]["written"]
        rates = {
            k.removeprefix("nulls__"): v / n
            for k, v in m["profile"].items()
            if k.startswith("nulls__")
        }
        if m["vintage_year"] == year:
            this = rates
        else:
            for col, rate in rates.items():
                others.setdefault(col, []).append(rate)

    if not this or not others:
        return GateResult(
            f"null_rate_drift__{dataset}", True, WARNING,
            "insufficient vintages for comparison",
        )

    drifted = {}
    for col, rate in this.items():
        peers = others.get(col)
        if not peers:
            continue
        med = statistics.median(peers)
        if abs(rate - med) > tolerance:
            drifted[col] = {"this": round(rate, 4), "median_other": round(med, 4)}

    return GateResult(
        f"null_rate_drift__{dataset}",
        not drifted,
        WARNING,
        "" if not drifted else f"null-rate drift beyond {tolerance:.0%}: {drifted}",
        metrics=drifted,
    )


def validate_bronze(settings: Settings, year: int) -> list[GateResult]:
    results = [
        gate_row_accounting(settings, "origination", year),
        gate_row_accounting(settings, "performance", year),
        gate_primary_key_unique(settings, year),
        gate_referential_integrity(settings, year),
        gate_vintage_consistency(settings, "origination", year),
        gate_vintage_consistency(settings, "performance", year),
        gate_loan_age_monotonic(settings, year),
        gate_terminal_event_once(settings, year),
        gate_no_sentinel_leakage(settings, year),
        gate_range_and_enum_violations(settings, "origination", year),
        gate_range_and_enum_violations(settings, "performance", year),
        gate_null_rate_drift(settings, "origination", year),
        gate_null_rate_drift(settings, "performance", year),
    ]
    return results


# ---------------------------------------------------------------------------
# Curated gates
# ---------------------------------------------------------------------------


def gate_severity_reconciliation(
    settings: Settings, year: int, tolerance: float = 0.02, min_agreement: float = 0.99
) -> GateResult:
    """Reconstructed loss must agree with Freddie's ACTUAL_LOSS_CALCULATION.

    Two independent routes to the same number: if they agree, the recovery and
    expense component handling is right. If they diverge, one of the sign
    conventions is wrong -- the most likely silent error in the severity path.
    """
    lf = scan_dataset(settings.curated / "loan_outcomes").filter(
        pl.col("vintage_year") == year
    )
    # Only loans with a numeric NET_SALE_PROCEEDS can be reconciled: the C and U
    # codes leave the recovery side unquantified, so RECONSTRUCTED_LOSS is null
    # there by design and comparing it would be meaningless.
    disposed = lf.filter(
        pl.col("ACTUAL_LOSS_CALCULATION").is_not_null()
        & (pl.col("ACTUAL_LOSS_CALCULATION") != 0)
        & pl.col("RECONSTRUCTED_LOSS").is_not_null()
    )
    stats = (
        disposed.with_columns(
            (
                (pl.col("RECONSTRUCTED_LOSS") - pl.col("ACTUAL_LOSS_CALCULATION")).abs()
                / pl.col("ACTUAL_LOSS_CALCULATION").abs()
            ).alias("_rel_err")
        )
        .select(
            pl.len().alias("n"),
            (pl.col("_rel_err") <= tolerance).sum().alias("n_agree"),
            pl.col("_rel_err").median().alias("median_rel_err"),
        )
        .collect()
        .to_dicts()[0]
    )

    n = stats["n"]
    if n == 0:
        return GateResult(
            "severity_reconciliation", True, WARNING,
            f"no disposed loans with a loss in vintage {year}",
        )
    share = stats["n_agree"] / n
    ok = share >= min_agreement
    return GateResult(
        "severity_reconciliation",
        ok,
        ERROR,
        "" if ok else f"only {share:.1%} of {n} disposed loans reconcile within "
                      f"{tolerance:.0%} (median rel. error {stats['median_rel_err']})",
        metrics={**stats, "agreement_share": share},
    )


def gate_macro_coverage(settings: Settings, year: int, min_coverage: float = 0.99) -> GateResult:
    """Loan-months that failed to pick up state unemployment.

    Catches the class of bug where a state is missing from the series list --
    DC was absent from the old 50-state list, silently nulling every DC loan.
    """
    path = settings.curated / "loan_month"
    lf = scan_dataset(path).filter(pl.col("vintage_year") == year)
    if "UNEMPLOYMENT_STATE" not in lf.collect_schema().names():
        return GateResult(
            "macro_coverage", True, WARNING,
            "panel built without macro data (--skip-macro); coverage not checked",
        )
    stats = lf.select(
        pl.len().alias("n"),
        pl.col("UNEMPLOYMENT_STATE").is_null().sum().alias("n_missing"),
    ).collect().to_dicts()[0]
    if stats["n"] == 0:
        return GateResult("macro_coverage", True, WARNING, "no rows")

    missing_states = (
        lf.filter(pl.col("UNEMPLOYMENT_STATE").is_null())
        .select(pl.col("PROPERTY_STATE").unique())
        .collect()["PROPERTY_STATE"]
        .to_list()
    )
    coverage = 1 - stats["n_missing"] / stats["n"]
    known_uncovered = {"GU", "VI"}
    unexpected = sorted(set(missing_states) - known_uncovered - {None})
    ok = coverage >= min_coverage and not unexpected
    return GateResult(
        "macro_coverage",
        ok,
        ERROR if unexpected else WARNING,
        "" if ok else f"coverage {coverage:.2%}; unexpected states without macro data: {unexpected}",
        metrics={"coverage": coverage, "unexpected_states": unexpected},
    )


def gate_all_columns_classified(settings: Settings, year: int) -> GateResult:
    """Every panel column must have a declared role in columns.yaml.

    An unclassified column is one nobody has decided is safe to train on. Since
    the panel deliberately carries post-outcome fields, the default must be
    "refuse", not "assume it's a feature".
    """
    from .features import load_registry, unclassified_columns

    cols = scan_dataset(settings.curated / "loan_month").collect_schema().names()
    stray = unclassified_columns(cols, settings.schema_dir)
    counts = {
        role: len(v)
        for role, v in load_registry(settings.schema_dir).classify(cols).items()
        if v
    }
    return GateResult(
        "all_columns_classified",
        not stray,
        ERROR,
        "" if not stray else f"{len(stray)} column(s) with no role in columns.yaml: {stray}",
        metrics={"role_counts": counts, "unclassified": stray},
    )


def gate_features_exclude_leakage(settings: Settings, year: int) -> GateResult:
    """No target or post-outcome column may survive feature selection.

    The concrete failure this prevents: training a default model on a frame
    still containing ACTUAL_LOSS_CALCULATION or ZERO_BALANCE_CODE, which
    produces a flawless in-sample model with no predictive value.
    """
    from .features import load_registry, select

    reg = load_registry(settings.schema_dir)
    cols = scan_dataset(settings.curated / "loan_month").collect_schema().names()

    offenders: dict[str, list[str]] = {}
    for alias in reg.target_groups:
        try:
            sel = select(cols, alias, schema_dir=settings.schema_dir)
        except Exception as exc:  # target absent from this panel build
            offenders.setdefault("_unresolvable", []).append(f"{alias}: {exc}")
            continue
        bad = [c for c in sel.features if reg.role_of(c) in ("target", "leakage")]
        if bad:
            offenders[alias] = bad

    return GateResult(
        "features_exclude_leakage",
        not offenders,
        ERROR,
        "" if not offenders else f"leakage reached the feature set: {offenders}",
        metrics={"targets_checked": len(reg.target_groups)},
    )


def gate_lagged_state_is_causal(settings: Settings, year: int) -> GateResult:
    """PRIOR_DLQ_MONTHS at t must equal DLQ_MONTHS at t-1, within each loan.

    Guards the shift/window logic. A silently mis-partitioned `.over()` would
    pull the previous *loan's* last month into the current loan's first row.
    """
    lf = scan_dataset(settings.curated / "loan_month").filter(pl.col("vintage_year") == year)
    mismatches = (
        lf.sort(["LOAN_SEQUENCE_NUMBER", "MONTHLY_REPORTING_PERIOD"])
        .with_columns(
            pl.col("DLQ_MONTHS")
            .shift(1)
            .over(partition_by="LOAN_SEQUENCE_NUMBER", order_by="MONTHLY_REPORTING_PERIOD")
            .alias("_expected")
        )
        .filter(
            pl.col("PRIOR_DLQ_MONTHS").is_not_null().or_(pl.col("_expected").is_not_null())
            & (pl.col("PRIOR_DLQ_MONTHS").ne_missing(pl.col("_expected")))
        )
        .select(pl.len())
        .collect()
        .item()
    )
    # The first row of each loan must have no prior state.
    leaked_first = (
        lf.filter(pl.col("IS_FIRST_OBSERVATION") & pl.col("PRIOR_UPB").is_not_null())
        .select(pl.len())
        .collect()
        .item()
    )
    ok = mismatches == 0 and leaked_first == 0
    return GateResult(
        "lagged_state_is_causal",
        ok,
        ERROR,
        "" if ok else f"{mismatches} lag mismatch(es), {leaked_first} first-row leak(s)",
        metrics={"lag_mismatches": mismatches, "first_row_leaks": leaked_first},
    )


def validate_curated(settings: Settings, year: int) -> list[GateResult]:
    return [
        gate_severity_reconciliation(settings, year),
        gate_macro_coverage(settings, year),
        gate_all_columns_classified(settings, year),
        gate_features_exclude_leakage(settings, year),
        gate_lagged_state_is_causal(settings, year),
    ]