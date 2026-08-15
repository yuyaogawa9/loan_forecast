"""Conditional amount models: how much, given that an outcome occurred.

The transition models answer *which* outcome a loan reaches. They say nothing
about magnitude, and until now every amount in the simulation was either a flat
assumption or a coarse bucketed table.

WHY QUANTILE REGRESSION RATHER THAN A CONDITIONAL MEAN
------------------------------------------------------
Monte Carlo needs a distribution, not a point estimate. Realised loss severity
runs from -7.9% at the 1st percentile (a disposition can produce a gain) to
+147.8% at the 99th, with 756 negative observations and 1,540 above 100%. A mean
model plus a homoscedastic residual cannot represent that, and a parametric fit
(Beta, Normal) is wrong on its face -- the support is neither [0,1] nor
symmetric.

Fitting a grid of quantiles gives a conditional distribution whose SHAPE varies
with the covariates, which is what a bucketed empirical table can only crudely
approximate before its cells go thin. Sampling then inverts the predicted
quantile curve against a uniform.

QUANTILE CROSSING
-----------------
Quantiles fitted independently are not guaranteed monotone: the predicted 75th
can land below the predicted 50th for some rows. That is a well-known artefact,
and left alone it yields a non-monotone inverse CDF -- i.e. invalid draws. It is
repaired by sorting each row's predicted curve, which is the standard fix and
cannot make calibration worse (sorting is a projection onto the monotone cone).
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

import lightgbm as lgb
import numpy as np
import polars as pl

from loan_etl.io import utc_now
from loan_etl.settings import Settings

from .dataset import CategoricalEncoder, build_dataset, resolve_categoricals, to_arrow_features

# Wide enough at the ends to keep tail resolution: draws outside the outermost
# fitted quantiles saturate rather than extrapolate, so a narrow grid would clip
# exactly the tail a credit investor cares about.
DEFAULT_QUANTILES = (0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99)

DEFAULT_PARAMS = {
    "learning_rate": 0.05,
    "num_leaves": 31,
    "min_data_in_leaf": 100,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 1,
    "max_bin": 127,
    "num_threads": 6,
    "verbosity": -1,
}


class AmountError(RuntimeError):
    pass


def _predict(booster: lgb.Booster, table) -> np.ndarray:
    return np.asarray(
        booster.predict(table, num_iteration=booster.best_iteration), dtype=float
    )


@dataclass
class QuantileAmountModel:
    """One LightGBM booster per quantile, sampled by inverting the curve."""

    target: str
    quantiles: tuple[float, ...]
    boosters: list[lgb.Booster]
    features: list[str]
    encoder: CategoricalEncoder
    clip: tuple[float | None, float | None] = (None, None)
    metrics: dict[str, Any] = field(default_factory=dict)
    meta: dict[str, Any] = field(default_factory=dict)

    # --- scoring ----------------------------------------------------------
    def predict_quantiles(self, df: pl.DataFrame) -> np.ndarray:
        """(n, len(quantiles)) predicted curve, monotone by construction."""
        missing = [f for f in self.features if f not in df.columns]
        if missing:
            raise AmountError(f"{self.target}: missing features {missing}")

        table = to_arrow_features(df, self.features, self.encoder)
        curve = np.column_stack([_predict(b, table) for b in self.boosters])

        # Repair crossing BEFORE clipping, so the sort operates on the raw
        # predictions rather than on values already flattened against a bound.
        curve = np.sort(curve, axis=1)
        lo, hi = self.clip
        if lo is not None:
            curve = np.maximum(curve, lo)
        if hi is not None:
            curve = np.minimum(curve, hi)
        return curve

    def sample(self, df: pl.DataFrame, uniforms: np.ndarray) -> np.ndarray:
        """Draw one value per row by inverting the predicted quantile curve.

        Linear interpolation between grid points; beyond the outermost fitted
        quantiles the draw saturates rather than extrapolating, because a linear
        extrapolation off the 99th percentile of a fat-tailed loss distribution
        produces values with no support in the data.
        """
        if df.height == 0:
            return np.zeros(0, dtype=float)

        curve = self.predict_quantiles(df)
        grid = np.asarray(self.quantiles, dtype=float)
        u = np.clip(np.asarray(uniforms, dtype=float), grid[0], grid[-1])

        j = np.clip(np.searchsorted(grid, u, side="right"), 1, len(grid) - 1)
        rows = np.arange(df.height)
        lo_q, hi_q = grid[j - 1], grid[j]
        lo_v, hi_v = curve[rows, j - 1], curve[rows, j]
        weight = np.where(hi_q > lo_q, (u - lo_q) / (hi_q - lo_q), 0.0)
        return lo_v + weight * (hi_v - lo_v)

    def draw(self, frame: pl.DataFrame, uniforms: np.ndarray) -> np.ndarray:
        """Satisfies the `simulate.SeverityModel` protocol."""
        return self.sample(frame, uniforms)

    @property
    def median_index(self) -> int:
        return int(np.argmin(np.abs(np.asarray(self.quantiles) - 0.5)))

    def predict_median(self, df: pl.DataFrame) -> np.ndarray:
        return self.predict_quantiles(df)[:, self.median_index]

    # --- fitting ----------------------------------------------------------
    @classmethod
    def fit(
        cls,
        train: pl.DataFrame,
        target: str,
        features: Sequence[str],
        *,
        valid: pl.DataFrame | None = None,
        quantiles: Sequence[float] = DEFAULT_QUANTILES,
        weight_column: str | None = None,
        categorical_hint: Sequence[str] = (),
        clip: tuple[float | None, float | None] = (None, None),
        params: dict[str, Any] | None = None,
        num_boost_round: int = 400,
        early_stopping_rounds: int = 30,
    ) -> "QuantileAmountModel":
        features = [f for f in features if f in train.columns]
        if not features:
            raise AmountError(f"{target}: no usable features")
        if target not in train.columns:
            raise AmountError(f"{target}: target column absent")

        train = train.filter(pl.col(target).is_not_null())
        if train.height == 0:
            raise AmountError(f"{target}: no non-null observations to fit")
        if valid is not None:
            valid = valid.filter(pl.col(target).is_not_null())
            if valid.height == 0:
                valid = None

        encoder = CategoricalEncoder.fit(
            train, resolve_categoricals(train, features, hint=categorical_hint)
        )

        boosters, iterations = [], []
        started = time.perf_counter()
        for q in quantiles:
            # float label: an integral cast would truncate the target to zero
            dtrain = build_dataset(
                train, features, target, encoder=encoder,
                weight_column=weight_column, label_dtype=pl.Float32,
            )
            dvalid = (
                build_dataset(
                    valid, features, target, encoder=encoder,
                    weight_column=weight_column, reference=dtrain,
                    label_dtype=pl.Float32,
                )
                if valid is not None
                else None
            )
            booster = lgb.train(
                {**DEFAULT_PARAMS, **(params or {}), "objective": "quantile",
                 "alpha": float(q), "metric": "quantile"},
                dtrain,
                num_boost_round=num_boost_round,
                valid_sets=[d for d in (dvalid,) if d is not None],
                valid_names=["valid"],
                callbacks=(
                    [lgb.early_stopping(early_stopping_rounds, verbose=False)]
                    if dvalid is not None
                    else []
                ),
            )
            boosters.append(booster)
            iterations.append(booster.best_iteration or booster.current_iteration())

        model = cls(
            target=target,
            quantiles=tuple(float(q) for q in quantiles),
            boosters=boosters,
            features=features,
            encoder=encoder,
            clip=clip,
            metrics={
                "train_rows": train.height,
                "valid_rows": 0 if valid is None else valid.height,
                "n_features": len(features),
                "iterations": iterations,
                "fit_seconds": round(time.perf_counter() - started, 2),
            },
        )
        if valid is not None:
            model.metrics["calibration"] = quantile_calibration(model, valid).to_dicts()
        return model


def quantile_calibration(
    model: QuantileAmountModel, df: pl.DataFrame, weight_column: str | None = None
) -> pl.DataFrame:
    """Share of actuals falling below each predicted quantile.

    THE test for a quantile model. A curve can fit the conditional median well
    and still be a useless distribution; only coverage establishes that the
    spread is right, and the spread is the entire reason to fit quantiles.
    Coverage at level q should come out at q.
    """
    df = df.filter(pl.col(model.target).is_not_null())
    if df.height == 0:
        return pl.DataFrame()

    curve = model.predict_quantiles(df)
    actual = df[model.target].cast(pl.Float64).to_numpy()
    weights = (
        df[weight_column].cast(pl.Float64).to_numpy()
        if weight_column and weight_column in df.columns
        else np.ones(df.height)
    )
    total = weights.sum()

    return pl.DataFrame(
        [
            {
                "quantile": q,
                "coverage": float((weights * (actual <= curve[:, k])).sum() / total),
                "error": float((weights * (actual <= curve[:, k])).sum() / total - q),
                "predicted_mean": float(curve[:, k].mean()),
                "n": df.height,
            }
            for k, q in enumerate(model.quantiles)
        ]
    )


@dataclass
class BinaryHazardModel:
    """Monthly probability that an auxiliary event occurs.

    Separate from the transition multinomials because a curtailment is not a
    state change: a loan can be CURRENT and still prepay part of its balance, so
    it cannot be a destination in a row-stochastic matrix.
    """

    target: str
    booster: lgb.Booster
    features: list[str]
    encoder: CategoricalEncoder
    metrics: dict[str, Any] = field(default_factory=dict)
    meta: dict[str, Any] = field(default_factory=dict)

    def predict(self, df: pl.DataFrame) -> np.ndarray:
        missing = [f for f in self.features if f not in df.columns]
        if missing:
            raise AmountError(f"{self.target}: missing features {missing}")
        return _predict(self.booster, to_arrow_features(df, self.features, self.encoder))

    def occurs(self, df: pl.DataFrame, uniforms: np.ndarray) -> np.ndarray:
        return np.asarray(uniforms, dtype=float) < self.predict(df)

    @classmethod
    def fit(
        cls,
        train: pl.DataFrame,
        target: str,
        features: Sequence[str],
        *,
        valid: pl.DataFrame | None = None,
        weight_column: str | None = None,
        categorical_hint: Sequence[str] = (),
        params: dict[str, Any] | None = None,
        num_boost_round: int = 300,
        early_stopping_rounds: int = 30,
    ) -> "BinaryHazardModel":
        features = [f for f in features if f in train.columns]
        train = train.with_columns(pl.col(target).cast(pl.Int8).alias(target))
        if valid is not None:
            valid = valid.with_columns(pl.col(target).cast(pl.Int8).alias(target))

        encoder = CategoricalEncoder.fit(
            train, resolve_categoricals(train, features, hint=categorical_hint)
        )
        dtrain = build_dataset(
            train, features, target, encoder=encoder, weight_column=weight_column
        )
        dvalid = (
            build_dataset(
                valid, features, target, encoder=encoder,
                weight_column=weight_column, reference=dtrain,
            )
            if valid is not None and valid.height
            else None
        )
        started = time.perf_counter()
        booster = lgb.train(
            {**DEFAULT_PARAMS, **(params or {}), "objective": "binary", "metric": "binary_logloss"},
            dtrain,
            num_boost_round=num_boost_round,
            valid_sets=[d for d in (dvalid,) if d is not None],
            valid_names=["valid"],
            callbacks=(
                [lgb.early_stopping(early_stopping_rounds, verbose=False)]
                if dvalid is not None
                else []
            ),
        )
        metrics = {
            "train_rows": train.height,
            "valid_rows": 0 if valid is None else valid.height,
            "n_features": len(features),
            "best_iteration": booster.best_iteration or booster.current_iteration(),
            "fit_seconds": round(time.perf_counter() - started, 2),
            "train_rate": float(train[target].mean()),
        }
        model = cls(
            target=target, booster=booster, features=features,
            encoder=encoder, metrics=metrics,
        )
        if dvalid is not None:
            predicted = float(model.predict(valid).mean())
            metrics["valid_predicted_rate"] = predicted
            metrics["valid_actual_rate"] = float(valid[target].mean())
        return model


# --- persistence ------------------------------------------------------------
#
# Same self-describing shape as `registry.save_model`: a booster alone cannot
# score new data correctly without its feature order and categorical encoding.


def amount_dir(settings: Settings, name: str, target: str | None = None) -> Path:
    base = settings.data_root / "model" / "registry" / name
    return base / f"AMOUNT={target}" if target else base


def save_amount_model(
    settings: Settings, name: str, model: QuantileAmountModel | BinaryHazardModel
) -> Path:
    d = amount_dir(settings, name, model.target)
    d.mkdir(parents=True, exist_ok=True)

    payload: dict[str, Any] = {
        "target": model.target,
        "features": model.features,
        "encoder": model.encoder.to_dict(),
        "metrics": model.metrics,
        "meta": model.meta,
        "saved_utc": utc_now(),
    }
    if isinstance(model, QuantileAmountModel):
        payload["kind"] = "quantile"
        payload["quantiles"] = list(model.quantiles)
        payload["clip"] = list(model.clip)
        for k, booster in enumerate(model.boosters):
            booster.save_model(str(d / f"model_q{k}.txt"))
    else:
        payload["kind"] = "binary"
        model.booster.save_model(str(d / "model.txt"))

    (d / "meta.json").write_text(json.dumps(payload, indent=2, default=str))
    return d


def load_amount_model(
    settings: Settings, name: str, target: str
) -> QuantileAmountModel | BinaryHazardModel:
    d = amount_dir(settings, name, target)
    meta_path = d / "meta.json"
    if not meta_path.exists():
        raise FileNotFoundError(f"No amount model at {d}. Train it first.")

    payload = json.loads(meta_path.read_text())
    encoder = CategoricalEncoder.from_dict(payload["encoder"])
    common = dict(
        target=payload["target"],
        features=payload["features"],
        encoder=encoder,
        metrics=payload.get("metrics", {}),
        meta=payload.get("meta", {}),
    )
    if payload.get("kind") == "binary":
        return BinaryHazardModel(booster=lgb.Booster(model_file=str(d / "model.txt")), **common)

    quantiles = tuple(payload["quantiles"])
    clip = payload.get("clip", [None, None])
    return QuantileAmountModel(
        quantiles=quantiles,
        boosters=[
            lgb.Booster(model_file=str(d / f"model_q{k}.txt")) for k in range(len(quantiles))
        ],
        clip=(clip[0], clip[1]),
        **common,
    )


def load_amount_models(settings: Settings, name: str) -> dict[str, Any]:
    base = amount_dir(settings, name)
    if not base.exists():
        return {}
    out = {}
    for d in sorted(base.glob("AMOUNT=*")):
        target = d.name.split("=", 1)[1]
        out[target] = load_amount_model(settings, name, target)
    return out
