"""Case-control sampling of the loan-month panel.

`CURRENT -> CURRENT` is 95.15% of the panel (70.0M of 73.6M rows) and carries
almost no information. Training on all of it is what makes the naive pipeline
need ~35 GB. Keeping every informative row and subsampling only that one
stratum brings the training set to ~7M rows, which fits comfortably in 8 GB and
loses nothing that matters.

Two design choices are load-bearing:

**Deterministic hashing, not `.sample()`.** The sampling decision is a pure
function of the row key, so it is reproducible across runs and machines, and --
crucially -- it composes with `collect(engine="streaming")`. `.sample()` needs
the frame resident to draw from, which is precisely what we cannot afford.

**Re-weighting, not post-hoc correction.** Each row carries
``SAMPLE_WEIGHT = 1 / rate``. LightGBM's weighted objective then targets the
population distribution directly, so predictions come out calibrated to the
real world without a separate prior-correction step. `evaluate.py` verifies
this against an unsampled holdout rather than assuming it.

Row-level sampling (rather than whole-loan) is correct here: every feature is
already a lagged, row-level quantity, so each loan-month is a valid independent
observation conditional on covariates.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from loan_etl.features import select
from loan_etl.io import scan_dataset, utc_now, write_manifest
from loan_etl.settings import Settings

from .states import TransitionConfig, load_transition_config

HASH_MODULUS = 1_000_000
STRATUM_KEYS = ["FROM_STATE", "EVENT"]


class SamplingError(RuntimeError):
    pass


def _hash_expr(cfg: TransitionConfig) -> pl.Expr:
    """Stable per-row hash in [0, HASH_MODULUS)."""
    cols = cfg.sampling["hash_columns"]
    key = pl.concat_str([pl.col(c).cast(pl.Utf8) for c in cols], separator="|")
    return key.hash(seed=int(cfg.sampling["seed"])) % HASH_MODULUS


def panel_with_states(settings: Settings, cfg: TransitionConfig | None = None) -> pl.LazyFrame:
    """The modelable panel, annotated with FROM_STATE and restricted to rows
    whose prior state can be conditioned on."""
    cfg = cfg or load_transition_config()
    return (
        scan_dataset(settings.curated / "loan_month")
        .filter(pl.col("IS_MODELABLE"))
        .with_columns(cfg.from_state_expr())
        .drop_nulls("FROM_STATE")
    )


def stratum_counts(lf: pl.LazyFrame) -> pl.DataFrame:
    """Population count per (from-state, event). Streams; never materialises."""
    return (
        lf.group_by(STRATUM_KEYS)
        .agg(pl.len().alias("n_population"))
        .collect(engine="streaming")
    )


def sampling_plan(counts: pl.DataFrame, cfg: TransitionConfig | None = None) -> pl.DataFrame:
    """Per-stratum sampling rate and the weight that undoes it."""
    cfg = cfg or load_transition_config()
    keep_all_below = int(cfg.sampling["keep_all_below"])
    majority_rate = float(cfg.sampling["majority_rate"])

    return counts.with_columns(
        pl.when(pl.col("n_population") < keep_all_below)
        .then(pl.lit(1.0))
        .otherwise(pl.lit(majority_rate))
        .alias("rate")
    ).with_columns(
        (1.0 / pl.col("rate")).alias("SAMPLE_WEIGHT"),
        (pl.col("n_population") * pl.col("rate")).round().cast(pl.Int64).alias("n_expected"),
    ).sort("n_population", descending=True)


def sample_columns(settings: Settings) -> list[str]:
    """Columns the training sample must carry.

    Deliberately the FULL feature set, not the Markov-safe subset: one sample
    serves both the projection models and any richer scoring model, so the
    expensive scan happens once.
    """
    panel_cols = scan_dataset(settings.curated / "loan_month").collect_schema().names()
    cols = select(panel_cols, "competing_risks")
    keep = [
        "LOAN_SEQUENCE_NUMBER",
        "MONTHLY_REPORTING_PERIOD",
        "vintage_year",
        "EVENT",
        "FROM_STATE",
        *cols.features,
    ]
    # Carried for severity/exposure work downstream; classified as leakage for
    # feature purposes, which is why they are named explicitly rather than
    # coming through select().
    for extra in ("PRIOR_UPB", "CURRENT_ACTUAL_UPB", "LOSS_SEVERITY"):
        if extra in panel_cols and extra not in keep:
            keep.append(extra)
    return list(dict.fromkeys(keep))


def build_training_sample(
    settings: Settings,
    cfg: TransitionConfig | None = None,
    *,
    force: bool = False,
) -> dict[str, Any]:
    """Build and persist the case-control sample, partitioned by from-state."""
    cfg = cfg or load_transition_config()
    out_base = settings.data_root / "model" / "training_sample"

    if out_base.exists() and not force:
        existing = sorted(out_base.glob("FROM_STATE=*/data.parquet"))
        if existing:
            n = pl.scan_parquet(out_base / "**" / "*.parquet").select(pl.len()).collect().item()
            return {"skipped": True, "rows": int(n), "partitions": len(existing)}

    lf = panel_with_states(settings, cfg)
    counts = stratum_counts(lf)
    plan = sampling_plan(counts, cfg)

    keep = [c for c in sample_columns(settings) if c in lf.collect_schema().names()]

    sampled = (
        lf.join(plan.lazy().select([*STRATUM_KEYS, "rate", "SAMPLE_WEIGHT"]), on=STRATUM_KEYS, how="left")
        .filter(_hash_expr(cfg) < (pl.col("rate") * HASH_MODULUS))
        .select([*keep, "SAMPLE_WEIGHT"])
    )

    out_base.mkdir(parents=True, exist_ok=True)

    # Drop partitions from a previous from-state definition. Changing the
    # bucketing (e.g. 06_PLUS -> 03_PLUS) orphans the old directories, and
    # anything globbing **/*.parquet would then read both generations and
    # double-count every affected row.
    valid = {f"FROM_STATE={fs}" for fs in cfg.from_states}
    stale = [p for p in out_base.glob("FROM_STATE=*") if p.name not in valid]
    for p in stale:
        for f in p.iterdir():
            f.unlink()
        p.rmdir()

    # One streaming pass over the 73.6M-row panel into a single staging file,
    # then cheap re-splits over the ~7M sampled rows. Sinking each from-state
    # directly would re-scan the whole panel once per state -- eight times the
    # I/O for the same result. polars 1.43's sink_parquet has no partition_by,
    # so staging is the way to keep it to a single expensive pass.
    staging = out_base / "_staging.parquet"
    sampled.sink_parquet(staging, compression="zstd", row_group_size=200_000)

    written = 0
    partitions: dict[str, int] = {}
    staged = pl.scan_parquet(staging)
    for from_state in cfg.from_states:
        part_dir = out_base / f"FROM_STATE={from_state}"
        part_dir.mkdir(parents=True, exist_ok=True)
        tmp = part_dir / "data.parquet.tmp"
        staged.filter(pl.col("FROM_STATE") == from_state).sink_parquet(
            tmp, compression="zstd", row_group_size=200_000
        )
        tmp.replace(part_dir / "data.parquet")
        n = pl.scan_parquet(part_dir / "data.parquet").select(pl.len()).collect().item()
        partitions[from_state] = int(n)
        written += int(n)
    staging.unlink(missing_ok=True)

    population = int(counts["n_population"].sum())
    manifest = {
        "stage": "training_sample",
        "created_utc": utc_now(),
        "seed": cfg.sampling["seed"],
        "keep_all_below": cfg.sampling["keep_all_below"],
        "majority_rate": cfg.sampling["majority_rate"],
        "population_rows": population,
        "sampled_rows": written,
        "reduction_factor": round(population / max(written, 1), 2),
        "rows_by_from_state": partitions,
        "strata": plan.to_dicts(),
        "output_path": str(out_base),
        "skipped": False,
    }
    write_manifest(settings.manifests, "model__training_sample", manifest)
    return manifest


def verify_reweighting(
    settings: Settings, cfg: TransitionConfig | None = None, tolerance: float = 0.02
) -> pl.DataFrame:
    """Re-weighted sample counts must reproduce population counts.

    This is the correctness proof for case-control sampling: if
    sum(SAMPLE_WEIGHT) per stratum does not recover the population count, the
    weights are wrong and every downstream probability is miscalibrated.
    """
    cfg = cfg or load_transition_config()
    out_base = settings.data_root / "model" / "training_sample"
    sample = pl.scan_parquet(out_base / "**" / "*.parquet")

    reweighted = (
        sample.group_by(STRATUM_KEYS)
        .agg(
            pl.len().alias("n_sampled"),
            pl.col("SAMPLE_WEIGHT").sum().round().cast(pl.Int64).alias("n_reweighted"),
        )
        .collect(engine="streaming")
    )
    population = stratum_counts(panel_with_states(settings, cfg))
    joined = population.join(reweighted, on=STRATUM_KEYS, how="left").with_columns(
        (
            (pl.col("n_reweighted") - pl.col("n_population")).abs()
            / pl.col("n_population")
        ).alias("rel_error")
    )
    return joined.with_columns((pl.col("rel_error") <= tolerance).alias("ok")).sort(
        "n_population", descending=True
    )
