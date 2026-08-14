"""Evaluation, centred on calibration against UNSAMPLED data.

Case-control sampling plus `1/rate` weights should reproduce population
transition rates. That is an assumption until it is checked, and if it is wrong
every projected probability is wrong with it. So the calibration check runs
against the raw panel, never the training sample.

The evaluation subsample is drawn UNIFORMLY, not stratified. Uniform sampling
leaves the class balance untouched, which is precisely the quantity being
tested; reusing the stratified training sample here would compare the model
against the distortion it is supposed to have corrected.
"""

from __future__ import annotations

from typing import Any, Sequence

import numpy as np
import polars as pl

from loan_etl.settings import Settings

from .registry import TransitionModel
from .sampling import HASH_MODULUS, panel_with_states
from .states import TransitionConfig, load_transition_config
from .transition import predict_destinations

EVAL_SEED = 990001


def _uniform_slice(lf: pl.LazyFrame, rate: float, seed: int = EVAL_SEED) -> pl.LazyFrame:
    if rate >= 1.0:
        return lf
    key = pl.concat_str(
        [pl.col("LOAN_SEQUENCE_NUMBER"), pl.col("MONTHLY_REPORTING_PERIOD")], separator="|"
    )
    return lf.filter((key.hash(seed=seed) % HASH_MODULUS) < rate * HASH_MODULUS)


def holdout_frame(
    settings: Settings,
    from_state: str,
    cfg: TransitionConfig,
    *,
    years: Sequence[int],
    rate: float = 1.0,
    columns: Sequence[str] | None = None,
) -> pl.DataFrame:
    """Unsampled holdout slice, projected to only the columns needed.

    The projection is not an optimisation, it is a requirement: collecting all
    129 panel columns for a multi-million-row slice exhausts memory and the
    process is OOM-killed. Only the model's features plus EVENT are needed.
    """
    lf = panel_with_states(settings, cfg).filter(pl.col("FROM_STATE") == from_state)
    year = pl.col("MONTHLY_REPORTING_PERIOD").str.slice(0, 4).cast(pl.Int32)
    lf = lf.filter(year.is_in(list(years)))
    lf = _uniform_slice(lf, rate)

    if columns is not None:
        available = set(lf.collect_schema().names())
        keep = [c for c in dict.fromkeys([*columns, "EVENT"]) if c in available]
        lf = lf.select(keep)
    return lf.collect(engine="streaming")


def calibration(
    settings: Settings,
    model: TransitionModel,
    cfg: TransitionConfig | None = None,
    *,
    years: Sequence[int] = (2022, 2023),
    rate: float = 1.0,
) -> pl.DataFrame:
    """Predicted vs actual transition rates on unsampled holdout data."""
    cfg = cfg or load_transition_config()
    df = holdout_frame(
        settings, model.from_state, cfg, years=years, rate=rate, columns=model.features
    )
    if df.height == 0:
        return pl.DataFrame()

    df = df.with_columns(cfg.label_expr(model.from_state))
    proba = predict_destinations(model, df)
    return _calibration_rows(model.from_state, model, proba, df["LABEL"].to_numpy())


def _calibration_rows(
    from_state: str, model: TransitionModel, proba: np.ndarray, actual: np.ndarray
) -> pl.DataFrame:
    n = len(actual)
    rows = []
    for dest, cls in sorted(model.label_map.items(), key=lambda kv: kv[1]):
        pred_rate = float(proba[:, cls].mean())
        act_rate = float((actual == cls).mean())
        rows.append(
            {
                "from_state": from_state,
                "destination": dest,
                "n": n,
                "actual_rate": act_rate,
                "predicted_rate": pred_rate,
                "abs_error": abs(pred_rate - act_rate),
                "rel_error": abs(pred_rate - act_rate) / act_rate if act_rate > 0 else None,
                "actual_count": int((actual == cls).sum()),
            }
        )
    return pl.DataFrame(rows)


def multiclass_logloss(proba: np.ndarray, labels: np.ndarray, eps: float = 1e-15) -> float:
    p = np.clip(proba[np.arange(len(labels)), labels], eps, 1.0)
    return float(-np.log(p).mean())


def one_vs_rest_auc(proba: np.ndarray, labels: np.ndarray, cls: int) -> float | None:
    """Rank-based AUC, computed directly to avoid a sklearn dependency here."""
    y = (labels == cls).astype(int)
    pos, neg = y.sum(), len(y) - y.sum()
    if pos == 0 or neg == 0:
        return None
    order = np.argsort(proba[:, cls])
    ranks = np.empty(len(y), dtype=float)
    ranks[order] = np.arange(1, len(y) + 1)
    return float((ranks[y == 1].sum() - pos * (pos + 1) / 2) / (pos * neg))


def score_from_state(
    settings: Settings,
    model: TransitionModel,
    cfg: TransitionConfig | None = None,
    *,
    years: Sequence[int] = (2022, 2023),
    rate: float = 1.0,
) -> dict[str, Any]:
    cfg = cfg or load_transition_config()
    df = holdout_frame(
        settings, model.from_state, cfg, years=years, rate=rate, columns=model.features
    )
    if df.height == 0:
        return {"from_state": model.from_state, "n": 0}

    df = df.with_columns(cfg.label_expr(model.from_state))
    proba = predict_destinations(model, df)
    labels = df["LABEL"].to_numpy()

    aucs = {
        dest: one_vs_rest_auc(proba, labels, cls)
        for dest, cls in sorted(model.label_map.items(), key=lambda kv: kv[1])
    }
    return {
        "from_state": model.from_state,
        "n": df.height,
        "multi_logloss": multiclass_logloss(proba, labels),
        "auc_by_destination": {k: (round(v, 4) if v is not None else None) for k, v in aucs.items()},
    }


def evaluate_all(
    settings: Settings,
    models: dict[str, TransitionModel],
    cfg: TransitionConfig | None = None,
    *,
    years: Sequence[int] = (2022, 2023),
    large_state_rate: float = 0.05,
    large_state_threshold: int = 2_000_000,
) -> tuple[pl.DataFrame, list[dict[str, Any]]]:
    """Calibration table plus per-state scores across every from-state.

    Loads each from-state's holdout ONCE and derives both outputs from it.
    Calling calibration() and score_from_state() separately would scan the whole
    4.2 GB panel twice per state -- sixteen passes for eight models.
    """
    cfg = cfg or load_transition_config()
    calib, scores = [], []
    for fs, model in models.items():
        declared_n = cfg.models[fs]["n"]
        rate = large_state_rate if declared_n > large_state_threshold else 1.0
        df = holdout_frame(
            settings, fs, cfg, years=years, rate=rate, columns=model.features
        )
        if df.height == 0:
            scores.append({"from_state": fs, "n": 0})
            continue

        df = df.with_columns(cfg.label_expr(fs))
        proba = predict_destinations(model, df)
        labels = df["LABEL"].to_numpy()

        calib.append(_calibration_rows(fs, model, proba, labels))
        scores.append(
            {
                "from_state": fs,
                "n": df.height,
                "multi_logloss": multiclass_logloss(proba, labels),
                "auc_by_destination": {
                    dest: (lambda v: round(v, 4) if v is not None else None)(
                        one_vs_rest_auc(proba, labels, cls)
                    )
                    for dest, cls in sorted(model.label_map.items(), key=lambda kv: kv[1])
                },
            }
        )
        del df, proba, labels

    table = pl.concat(calib) if calib else pl.DataFrame()
    return table, scores
