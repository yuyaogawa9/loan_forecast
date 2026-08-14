"""Per-from-state multinomial transition models.

One LightGBM multiclass model per from-state, each predicting only the
destinations that state actually reaches. That structure is what makes the
transition matrix row-stochastic by construction: a softmax over a from-state's
destinations sums to 1 without renormalisation, whereas a grid of independent
binary models would not.

Only the `00` model sees large data (~7M sampled rows); the rest fit in seconds.
"""

from __future__ import annotations

import time
from typing import Any

import lightgbm as lgb
import numpy as np
import polars as pl

from loan_etl.features import select
from loan_etl.settings import Settings

from .dataset import (
    CategoricalEncoder,
    build_dataset,
    estimate_memory_gb,
    resolve_categoricals,
)
from .registry import TransitionModel, save_model, save_registry_index
from .states import TransitionConfig, load_transition_config

DEFAULT_MODEL_NAME = "transitions"


class TrainingError(RuntimeError):
    pass


def load_from_state_sample(settings: Settings, from_state: str) -> pl.DataFrame:
    path = settings.data_root / "model" / "training_sample" / f"FROM_STATE={from_state}"
    f = path / "data.parquet"
    if not f.exists():
        raise TrainingError(f"No training sample at {f}. Run the sample stage first.")
    return pl.read_parquet(f)


def time_split(df: pl.DataFrame, train_through_year: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Split by reporting year, never randomly.

    A random split would put the same loan's adjacent months on both sides and
    leak; it would also flatter the model by letting it train on the future.
    """
    year = pl.col("MONTHLY_REPORTING_PERIOD").str.slice(0, 4).cast(pl.Int32)
    return df.filter(year <= train_through_year), df.filter(year > train_through_year)


def train_from_state(
    settings: Settings,
    from_state: str,
    cfg: TransitionConfig | None = None,
    *,
    markov_safe: bool = True,
    panel_columns: list[str] | None = None,
) -> TransitionModel:
    cfg = cfg or load_transition_config()
    df = load_from_state_sample(settings, from_state)

    cols = select(
        panel_columns or df.columns,
        "competing_risks",
        markov_safe_only=markov_safe,
    )
    features = [f for f in cols.features if f in df.columns]

    # Collapse EVENT to this state's destination set and map to class indices.
    label_map = cfg.label_map(from_state)
    df = df.with_columns(cfg.label_expr(from_state))
    unlabelled = df["LABEL"].null_count()
    if unlabelled:
        raise TrainingError(
            f"{from_state}: {unlabelled} rows could not be mapped to a destination"
        )

    train_df, valid_df = time_split(df, int(cfg.training["train_through_year"]))
    if train_df.height == 0:
        raise TrainingError(f"{from_state}: empty training split")

    # Classes absent from the training split would leave dead softmax outputs
    # and shift every later class index, so the label space is pinned to the
    # config and verified rather than inferred from whatever happens to appear.
    present = set(train_df["LABEL"].unique().to_list())
    missing = sorted(set(label_map.values()) - present)
    if missing:
        inverse = {v: k for k, v in label_map.items()}
        raise TrainingError(
            f"{from_state}: destinations absent from the training split: "
            f"{[inverse[m] for m in missing]}. Widen the split or revisit the "
            "collapse map in config/transitions.yaml."
        )

    encoder = CategoricalEncoder.fit(
        train_df, resolve_categoricals(train_df, features, hint=cols.categorical)
    )

    dtrain = build_dataset(
        train_df, features, "LABEL", encoder=encoder, weight_column="SAMPLE_WEIGHT"
    )
    dvalid = (
        build_dataset(
            valid_df, features, "LABEL", encoder=encoder,
            weight_column="SAMPLE_WEIGHT", reference=dtrain,
        )
        if valid_df.height
        else None
    )

    params = {
        **cfg.training["lightgbm"],
        "objective": "multiclass",
        "num_class": len(label_map),
        "metric": "multi_logloss",
    }
    t0 = time.perf_counter()
    evals: dict[str, dict[str, list[float]]] = {}
    booster = lgb.train(
        params,
        dtrain,
        num_boost_round=int(cfg.training["num_boost_round"]),
        valid_sets=[d for d in (dvalid,) if d is not None],
        valid_names=["valid"],
        callbacks=(
            [
                lgb.early_stopping(int(cfg.training["early_stopping_rounds"]), verbose=False),
                lgb.record_evaluation(evals),
            ]
            if dvalid is not None
            else [lgb.record_evaluation(evals)]
        ),
    )
    elapsed = time.perf_counter() - t0

    metrics: dict[str, Any] = {
        "train_rows": train_df.height,
        "valid_rows": valid_df.height,
        "n_features": len(features),
        "n_classes": len(label_map),
        "best_iteration": booster.best_iteration or booster.current_iteration(),
        "fit_seconds": round(elapsed, 2),
        "est_feature_matrix_gb": round(
            estimate_memory_gb(train_df.height, len(features)), 3
        ),
    }
    if evals.get("valid", {}).get("multi_logloss"):
        metrics["valid_multi_logloss"] = float(evals["valid"]["multi_logloss"][-1])

    return TransitionModel(
        from_state=from_state,
        booster=booster,
        features=features,
        label_map=label_map,
        encoder=encoder,
        markov_safe=markov_safe,
        metrics=metrics,
        meta={
            "config_version": cfg.version,
            "train_through_year": cfg.training["train_through_year"],
            "params": params,
        },
    )


def train_all(
    settings: Settings,
    cfg: TransitionConfig | None = None,
    *,
    name: str = DEFAULT_MODEL_NAME,
    markov_safe: bool = True,
    only: list[str] | None = None,
) -> dict[str, Any]:
    cfg = cfg or load_transition_config()
    targets = only or list(cfg.from_states)
    results: dict[str, Any] = {}

    for fs in targets:
        model = train_from_state(settings, fs, cfg, markov_safe=markov_safe)
        save_model(settings, name, model)
        results[fs] = model.metrics
        print(
            f"  {fs:8s} classes={model.metrics['n_classes']:<2} "
            f"train={model.metrics['train_rows']:>9,} "
            f"valid={model.metrics['valid_rows']:>8,} "
            f"iters={model.metrics['best_iteration']:>4} "
            f"logloss={model.metrics.get('valid_multi_logloss', float('nan')):.5f} "
            f"{model.metrics['fit_seconds']:>6.1f}s"
        )

    index = {
        "name": name,
        "markov_safe": markov_safe,
        "config_version": cfg.version,
        "from_states": targets,
        "metrics": results,
    }
    save_registry_index(settings, name, index)
    return index
