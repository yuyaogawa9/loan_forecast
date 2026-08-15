"""State space, from-state bucketing and destination collapse.

Loaded from ``config/transitions.yaml`` so the state space is data, not code --
the same pattern the ETL uses for field schemas.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

import polars as pl
import yaml

from loan_etl.settings import SCHEMA_DIR

CONFIG_PATH = SCHEMA_DIR.parent / "transitions.yaml"


class StateError(RuntimeError):
    pass


@dataclass(frozen=True)
class TransitionConfig:
    version: int
    transient_states: tuple[str, ...]
    absorbing_states: tuple[str, ...]
    fallback_state: str
    thresholds: dict[str, int]
    from_state: dict[str, Any]
    models: dict[str, dict[str, Any]]
    sampling: dict[str, Any]
    training: dict[str, Any]
    simulation: dict[str, Any] = field(default_factory=dict)
    amounts: dict[str, Any] = field(default_factory=dict)

    @property
    def principal_paid_factor(self) -> dict[str, float]:
        """Share of scheduled principal actually paid, by delinquency months."""
        return {
            str(k): float(v)
            for k, v in (self.simulation.get("principal_paid_factor") or {}).items()
        }

    # --- State space ------------------------------------------------------
    @property
    def all_states(self) -> tuple[str, ...]:
        return tuple(self.transient_states) + tuple(self.absorbing_states)

    @property
    def from_states(self) -> tuple[str, ...]:
        return tuple(self.models)

    def destinations(self, from_state: str) -> tuple[str, ...]:
        try:
            return tuple(self.models[from_state]["destinations"])
        except KeyError as exc:
            raise StateError(
                f"Unknown from-state {from_state!r}. Known: {list(self.models)}"
            ) from exc

    def label_map(self, from_state: str) -> dict[str, int]:
        """Destination -> contiguous class index, as LightGBM requires."""
        return {d: i for i, d in enumerate(self.destinations(from_state))}

    def is_absorbing(self, state: str) -> bool:
        return state in self.absorbing_states

    def model_key_for_state(self, state: str) -> str:
        """Transient state label -> from-state model key.

        The projection works in named states (CURRENT, DLQ_30, ...) while models
        are keyed by Freddie's delinquency codes. This is the single bridge
        between the two; `load_transition_config` asserts it is total.
        """
        deep = self.from_state["deep_delinquency_label"]
        return {
            "CURRENT": "00",
            "DLQ_30": "01",
            "DLQ_60": "02",
            "DLQ_90_PLUS": deep,
            "REO": self.from_state["reo_code"],
        }.get(state, state)

    # --- From-state bucketing --------------------------------------------
    def bucket_from_state(self, status: str | None) -> str | None:
        """Map a raw PRIOR_DLQ_STATUS to a modelled from-state.

        Returns None for statuses that cannot be conditioned on: null (Freddie
        reported "XX", unknown) or anything unparseable.
        """
        if status is None:
            return None
        status = status.strip()
        if status == self.from_state["reo_code"]:
            return self.from_state["reo_code"]
        try:
            months = int(status)
        except ValueError:
            return None
        if months >= self.from_state["deep_delinquency_min_months"]:
            return self.from_state["deep_delinquency_label"]
        return f"{months:02d}"

    def from_state_expr(self, column: str | None = None) -> pl.Expr:
        """Vectorised equivalent of bucket_from_state, for lazy pipelines."""
        col = pl.col(column or self.from_state["source_column"]).str.strip_chars()
        reo = self.from_state["reo_code"]
        deep_min = self.from_state["deep_delinquency_min_months"]
        deep_label = self.from_state["deep_delinquency_label"]
        months = col.cast(pl.Int32, strict=False)
        return (
            pl.when(col == reo)
            .then(pl.lit(reo))
            .when(months.is_null())
            .then(pl.lit(None, pl.Utf8))
            .when(months >= deep_min)
            .then(pl.lit(deep_label))
            .otherwise(months.cast(pl.Utf8).str.zfill(2))
            .alias("FROM_STATE")
        )

    def destination_expr(self, from_state: str, column: str = "EVENT") -> pl.Expr:
        """Collapse EVENT to this from-state's destination set.

        Anything outside the set becomes the fallback absorbing state, so
        probability mass is conserved. Dropping instead would leave the
        transition matrix non-stochastic.
        """
        allowed = list(self.destinations(from_state))
        return (
            pl.when(pl.col(column).is_in(allowed))
            .then(pl.col(column))
            .otherwise(pl.lit(self.fallback_state))
            .alias("DESTINATION")
        )

    def label_expr(self, from_state: str, column: str = "EVENT") -> pl.Expr:
        """Collapsed destination as a contiguous integer class index."""
        mapping = self.label_map(from_state)
        return (
            self.destination_expr(from_state, column)
            .replace_strict(mapping, default=None, return_dtype=pl.Int32)
            .alias("LABEL")
        )


@lru_cache(maxsize=2)
def load_transition_config(path: Path | None = None) -> TransitionConfig:
    p = Path(path) if path else CONFIG_PATH
    if not p.exists():
        raise StateError(f"No transition config at {p}")
    spec = yaml.safe_load(p.read_text())

    cfg = TransitionConfig(
        version=spec["version"],
        transient_states=tuple(spec["transient_states"]),
        absorbing_states=tuple(spec["absorbing_states"]),
        fallback_state=spec["fallback_state"],
        thresholds=dict(spec["thresholds"]),
        from_state=dict(spec["from_state"]),
        models={k: dict(v) for k, v in spec["models"].items()},
        sampling=dict(spec["sampling"]),
        training=dict(spec["training"]),
        simulation=dict(spec.get("simulation") or {}),
        amounts=dict(spec.get("amounts") or {}),
    )

    known = set(cfg.all_states)
    if cfg.fallback_state not in cfg.absorbing_states:
        raise StateError(f"fallback_state {cfg.fallback_state!r} must be absorbing")
    overlap = set(cfg.transient_states) & set(cfg.absorbing_states)
    if overlap:
        raise StateError(f"states cannot be both transient and absorbing: {sorted(overlap)}")

    for fs, body in cfg.models.items():
        dests = body["destinations"]
        unknown = sorted(set(dests) - known)
        if unknown:
            raise StateError(f"from-state {fs}: unknown destinations {unknown}")
        if len(set(dests)) != len(dests):
            raise StateError(f"from-state {fs}: duplicate destinations")
        if cfg.fallback_state not in dests:
            raise StateError(
                f"from-state {fs}: must include {cfg.fallback_state} so folded "
                "rare destinations still carry their probability mass"
            )
    # Every transient state must be reachable from somewhere, or the chain has
    # an unreachable node and the projection can never enter it.
    reachable = {d for b in cfg.models.values() for d in b["destinations"]}
    stranded = sorted(set(cfg.transient_states) - reachable)
    if stranded:
        raise StateError(f"transient states unreachable from any from-state: {stranded}")

    # THE ALPHABETS MUST MATCH. Projection occupies transient STATES, but the
    # models are keyed by from-state; every transient state therefore has to map
    # onto a real model, and every model has to be reachable as a state.
    #
    # Violating this is silent and catastrophic. v1 bucketed from-states at 6
    # months while EVENT collapses at 3, so a loan in DLQ_90_PLUS always used
    # the from-03 model -- which had no CHARGEOFF or REO destination. Models 04,
    # 05 and 06_PLUS could never fire, default mass cycled in DLQ_90_PLUS
    # forever, and projected losses came out as exactly zero with no error.
    unmapped = sorted(
        s for s in cfg.transient_states if cfg.model_key_for_state(s) not in cfg.models
    )
    if unmapped:
        raise StateError(
            f"transient states with no model: {unmapped}. The from-state buckets "
            "must collapse at the same point as the EVENT destinations, or those "
            "states become dead ends that cannot reach their terminal events."
        )
    unreachable_models = sorted(
        fs
        for fs in cfg.models
        if fs not in {cfg.model_key_for_state(s) for s in cfg.transient_states}
    )
    if unreachable_models:
        raise StateError(
            f"models that no transient state routes to: {unreachable_models}. "
            "They would never fire during projection."
        )
    return cfg


def verify_against_data(lf: pl.LazyFrame, cfg: TransitionConfig | None = None) -> pl.DataFrame:
    """Recompute observed destination shares and report drift from the config.

    The config pins counts derived at build time; this is how you find out the
    real distribution has moved (a new vintage, a re-run of the ETL) without
    the config silently going stale.
    """
    cfg = cfg or load_transition_config()
    observed = (
        lf.filter(pl.col("IS_MODELABLE"))
        .with_columns(cfg.from_state_expr())
        .drop_nulls("FROM_STATE")
        .group_by("FROM_STATE", "EVENT")
        .agg(pl.len().alias("n"))
        .collect(engine="streaming")
    )
    rows = []
    for fs in cfg.from_states:
        sub = observed.filter(pl.col("FROM_STATE") == fs)
        total = int(sub["n"].sum())
        allowed = set(cfg.destinations(fs))
        folded = int(sub.filter(~pl.col("EVENT").is_in(list(allowed)))["n"].sum())
        declared = cfg.models[fs]
        rows.append(
            {
                "from_state": fs,
                "n_observed": total,
                "n_declared": declared["n"],
                "n_drift": total - declared["n"],
                "folded_share_observed": folded / total if total else 0.0,
                "folded_share_declared": declared["folded_share"],
            }
        )
    return pl.DataFrame(rows)