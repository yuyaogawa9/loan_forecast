"""Transition matrix assembly.

Turns the per-from-state multinomials into a row-stochastic matrix over the
full state space. Each model emits a softmax over only the destinations its
from-state reaches, so those rows already sum to 1; this module places them into
the common ordering and makes absorbing rows exact identity.

Row-stochasticity is asserted, not assumed. A matrix whose rows drift from 1
leaks or creates probability mass at every projection step, and the error
compounds geometrically over a 360-month horizon.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import polars as pl

from .dataset import to_arrow_features
from .registry import TransitionModel
from .states import TransitionConfig

ROW_SUM_TOLERANCE = 1e-6


class TransitionError(RuntimeError):
    pass


@dataclass(frozen=True)
class StateSpace:
    """Common ordering shared by every matrix and state vector."""

    states: tuple[str, ...]
    transient: frozenset[str]
    absorbing: frozenset[str]

    @classmethod
    def from_config(cls, cfg: TransitionConfig) -> "StateSpace":
        return cls(
            states=cfg.all_states,
            transient=frozenset(cfg.transient_states),
            absorbing=frozenset(cfg.absorbing_states),
        )

    @property
    def size(self) -> int:
        return len(self.states)

    def index(self, state: str) -> int:
        try:
            return self.states.index(state)
        except ValueError as exc:
            raise TransitionError(f"Unknown state {state!r}") from exc

    def one_hot(self, state: str) -> np.ndarray:
        v = np.zeros(self.size)
        v[self.index(state)] = 1.0
        return v


def predict_destinations(model: TransitionModel, df: pl.DataFrame) -> np.ndarray:
    """Class probabilities for a batch, in the model's own class order."""
    missing = [f for f in model.features if f not in df.columns]
    if missing:
        raise TransitionError(f"{model.from_state}: missing features {missing}")
    table = to_arrow_features(df, model.features, model.encoder)
    proba = model.booster.predict(table, num_iteration=model.booster.best_iteration)
    return np.asarray(proba, dtype=float).reshape(df.height, len(model.label_map))


def build_matrix(
    models: dict[str, TransitionModel],
    covariates: dict[str, pl.DataFrame],
    space: StateSpace,
    cfg: TransitionConfig,
) -> np.ndarray:
    """Assemble one K x K transition matrix.

    ``covariates[from_state]`` is a single-row frame describing the loan (or
    cohort) conditional on being in that state. Absorbing states self-loop.
    """
    P = np.zeros((space.size, space.size))

    for state in space.states:
        i = space.index(state)
        if state in space.absorbing:
            P[i, i] = 1.0
            continue

        from_state = cfg.model_key_for_state(state)
        model = models.get(from_state)
        if model is None or from_state not in covariates:
            # A transient state with no model is a dead end: mass entering it can
            # never leave, so every downstream terminal event is under-counted
            # and losses silently project to zero. Refuse rather than self-loop.
            raise TransitionError(
                f"transient state {state!r} maps to from-state {from_state!r}, "
                "for which no model was supplied. Projection would trap all "
                "probability mass there and under-report every terminal event."
            )

        proba = predict_destinations(model, covariates[from_state])[0]
        for dest, cls in model.label_map.items():
            P[i, space.index(dest)] += proba[cls]

    validate_row_stochastic(P, space)
    return P


def validate_row_stochastic(P: np.ndarray, space: StateSpace) -> None:
    sums = P.sum(axis=1)
    bad = np.where(np.abs(sums - 1.0) > ROW_SUM_TOLERANCE)[0]
    if bad.size:
        detail = ", ".join(f"{space.states[i]}={sums[i]:.9f}" for i in bad[:5])
        raise TransitionError(f"transition matrix rows do not sum to 1: {detail}")
    if (P < -ROW_SUM_TOLERANCE).any():
        raise TransitionError("transition matrix contains negative probabilities")

    for state in space.absorbing:
        i = space.index(state)
        if abs(P[i, i] - 1.0) > ROW_SUM_TOLERANCE:
            raise TransitionError(
                f"absorbing state {state} must self-loop with probability 1, got {P[i, i]}"
            )


def to_frame(P: np.ndarray, space: StateSpace) -> pl.DataFrame:
    """Readable matrix, for inspection and for reporting roll rates."""
    return pl.DataFrame(
        {"FROM": list(space.states), **{s: P[:, j] for j, s in enumerate(space.states)}}
    )
