"""Loss given default, drawn from the realised severity distribution.

Named ``lgd`` rather than ``severity`` to keep it distinct from
`loan_etl.derive.severity`, which DERIVES realised severity from disposition
records. This module consumes that output and turns it into something a
simulation can sample.

A single flat severity -- what the matrix projection assumes -- makes every
defaulted loan lose the same fraction, so the only randomness left in the loss
total is default TIMING. That is the wrong shape for a credit investor: the tail
of a mortgage loss distribution is driven as much by how bad the bad ones are as
by how many there are. Drawing severity per path is what turns the simulation
output from a point estimate into a distribution with usable percentiles.

The estimator is deliberately non-parametric. Realised severity is not remotely
normal: it is fat-tailed, occasionally negative (a disposition can produce a
gain), and bounded below by nothing in particular. An empirical quantile function
reproduces all of that for free, where a fitted Beta or Normal would not.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Sequence

import numpy as np
import polars as pl

MIN_BUCKET_OBSERVATIONS = 50
# See `train_amounts.MIN_DEFAULTED_BALANCE`: severity is undefined once the
# defaulted balance approaches zero.
MIN_DEFAULTED_BALANCE = 1_000.0
DEFAULT_LTV_EDGES = (80.0, 100.0, 120.0)
FALLBACK_KEY = "__all__"


class SeverityError(RuntimeError):
    pass


def _ltv_band(edges: Sequence[float]) -> pl.Expr:
    """Mark-to-market LTV band, the strongest observable driver of severity.

    Equity position at default determines what the disposition recovers, so
    conditioning on it captures most of the systematic variation without fitting
    anything.
    """
    ltv = pl.col("MARK_TO_MARKET_LTV")
    expr = pl.when(ltv.is_null()).then(pl.lit("NA"))
    for i, edge in enumerate(edges):
        expr = expr.when(ltv <= edge).then(pl.lit(f"LTV_{i}"))
    return expr.otherwise(pl.lit(f"LTV_{len(edges)}")).alias("_LTV_BAND")


def bucket_expr(
    by_state: bool = True, edges: Sequence[float] = DEFAULT_LTV_EDGES
) -> pl.Expr:
    """The conditioning key: property state crossed with equity band."""
    parts = [_ltv_band(edges)]
    if by_state:
        parts.insert(0, pl.col("PROPERTY_STATE").fill_null("NA"))
    return pl.concat_str(parts, separator="|").alias("_SEVERITY_BUCKET")


@dataclass
class EmpiricalSeverity:
    """Per-bucket empirical quantile functions over realised LOSS_SEVERITY."""

    buckets: dict[str, np.ndarray]
    fallback: np.ndarray
    by_state: bool = True
    edges: tuple[float, ...] = DEFAULT_LTV_EDGES
    meta: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def fit(
        cls,
        observations: pl.DataFrame,
        *,
        by_state: bool = True,
        edges: Sequence[float] = DEFAULT_LTV_EDGES,
        min_observations: int = MIN_BUCKET_OBSERVATIONS,
    ) -> "EmpiricalSeverity":
        """Build from disposed loans carrying a realised LOSS_SEVERITY."""
        if "LOSS_SEVERITY" not in observations.columns:
            raise SeverityError("LOSS_SEVERITY is required to fit a severity model")

        obs = observations.filter(pl.col("LOSS_SEVERITY").is_not_null())
        if obs.height == 0:
            raise SeverityError("no rows with a realised LOSS_SEVERITY")

        fallback = np.sort(obs["LOSS_SEVERITY"].cast(pl.Float64).to_numpy())
        obs = obs.with_columns(bucket_expr(by_state, edges))

        buckets: dict[str, np.ndarray] = {}
        for (key,), part in obs.partition_by(
            "_SEVERITY_BUCKET", as_dict=True, include_key=False
        ).items():
            # Thin buckets fall back to the pooled distribution. A quantile
            # function built from eleven observations is noise, and the
            # simulation would resample that noise for every defaulted path in
            # the bucket.
            if part.height >= min_observations:
                buckets[str(key)] = np.sort(
                    part["LOSS_SEVERITY"].cast(pl.Float64).to_numpy()
                )

        return cls(
            buckets=buckets,
            fallback=fallback,
            by_state=by_state,
            edges=tuple(edges),
            meta={
                "n_observations": obs.height,
                "n_buckets": len(buckets),
                "min_observations": min_observations,
                "pooled_median": float(np.median(fallback)),
                "pooled_mean": float(fallback.mean()),
            },
        )

    def draw(self, frame: pl.DataFrame, uniforms: np.ndarray) -> np.ndarray:
        """Sample one severity per row, by inverse empirical CDF.

        Loops over BUCKETS, not rows -- a few hundred iterations regardless of
        how many paths defaulted this month.
        """
        if frame.height == 0:
            return np.zeros(0, dtype=float)

        keys = (
            frame.select(bucket_expr(self.by_state, self.edges))["_SEVERITY_BUCKET"]
            .to_numpy()
            .astype(object)
        )
        out = np.empty(frame.height, dtype=float)
        for key in np.unique(keys):
            rows = np.flatnonzero(keys == key)
            table = self.buckets.get(str(key), self.fallback)
            idx = np.clip(
                (uniforms[rows] * len(table)).astype(np.int64), 0, len(table) - 1
            )
            out[rows] = table[idx]
        return out

    # --- persistence ------------------------------------------------------
    def to_dict(self) -> dict[str, Any]:
        return {
            "buckets": {k: v.tolist() for k, v in self.buckets.items()},
            "fallback": self.fallback.tolist(),
            "by_state": self.by_state,
            "edges": list(self.edges),
            "meta": self.meta,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "EmpiricalSeverity":
        return cls(
            buckets={k: np.asarray(v, dtype=float) for k, v in payload["buckets"].items()},
            fallback=np.asarray(payload["fallback"], dtype=float),
            by_state=payload.get("by_state", True),
            edges=tuple(payload.get("edges", DEFAULT_LTV_EDGES)),
            meta=payload.get("meta", {}),
        )

    def summary(self) -> pl.DataFrame:
        rows = [
            {
                "bucket": key,
                "n": len(values),
                "p10": float(np.quantile(values, 0.10)),
                "median": float(np.median(values)),
                "mean": float(values.mean()),
                "p90": float(np.quantile(values, 0.90)),
            }
            for key, values in sorted(self.buckets.items())
        ]
        rows.append(
            {
                "bucket": FALLBACK_KEY,
                "n": len(self.fallback),
                "p10": float(np.quantile(self.fallback, 0.10)),
                "median": float(np.median(self.fallback)),
                "mean": float(self.fallback.mean()),
                "p90": float(np.quantile(self.fallback, 0.90)),
            }
        )
        return pl.DataFrame(rows)


def fit_from_panel(
    lf: pl.LazyFrame,
    *,
    by_state: bool = True,
    edges: Sequence[float] = DEFAULT_LTV_EDGES,
    min_observations: int = MIN_BUCKET_OBSERVATIONS,
) -> EmpiricalSeverity:
    """Fit from the curated panel, reading only the columns needed.

    Projecting first is not an optimisation here but a requirement: collecting
    the full panel to reach three columns is what gets the process OOM-killed.
    """
    wanted = ["LOSS_SEVERITY", "MARK_TO_MARKET_LTV", "PROPERTY_STATE"]
    available = set(lf.collect_schema().names())
    missing = [c for c in wanted if c not in available]
    if "LOSS_SEVERITY" in missing:
        raise SeverityError("panel has no LOSS_SEVERITY column")

    # Exclude loans whose defaulted balance was negligible. Severity is a ratio,
    # and nine observations in the panel divide a real loss by a balance of
    # $0.01-$517, yielding values up to 657,341. This sampler draws directly from
    # the observed array, so one of those would book a phantom loss larger than
    # the entire portfolio -- the quantile models are robust to it, this is not.
    keep = pl.col("LOSS_SEVERITY").is_not_null()
    if "ZERO_BALANCE_REMOVAL" in available:
        keep = keep & (pl.col("ZERO_BALANCE_REMOVAL") >= MIN_DEFAULTED_BALANCE)

    observations = (
        lf.filter(keep)
        .select([c for c in wanted if c in available])
        .collect(engine="streaming")
    )
    return EmpiricalSeverity.fit(
        observations, by_state=by_state, edges=edges, min_observations=min_observations
    )
