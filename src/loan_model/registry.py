"""Model persistence.

A booster alone is not a model. Reloading one and scoring new data correctly
also requires the exact feature order, the categorical encoding (frame-local
codes would otherwise drift), and the destination-label mapping. All of it is
stored together so a saved model is self-describing.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import lightgbm as lgb

from loan_etl.io import utc_now
from loan_etl.settings import Settings

from .dataset import CategoricalEncoder


@dataclass
class TransitionModel:
    from_state: str
    booster: lgb.Booster
    features: list[str]
    label_map: dict[str, int]        # destination -> class index
    encoder: CategoricalEncoder
    markov_safe: bool
    metrics: dict[str, Any]
    meta: dict[str, Any]

    @property
    def destinations(self) -> list[str]:
        """Destinations ordered by class index, matching predict() columns."""
        return [d for d, _ in sorted(self.label_map.items(), key=lambda kv: kv[1])]


def model_dir(settings: Settings, name: str, from_state: str | None = None) -> Path:
    base = settings.data_root / "model" / "registry" / name
    return base / f"FROM_STATE={from_state}" if from_state else base


def save_model(settings: Settings, name: str, model: TransitionModel) -> Path:
    d = model_dir(settings, name, model.from_state)
    d.mkdir(parents=True, exist_ok=True)
    model.booster.save_model(str(d / "model.txt"))
    (d / "meta.json").write_text(
        json.dumps(
            {
                "from_state": model.from_state,
                "features": model.features,
                "label_map": model.label_map,
                "encoder": model.encoder.to_dict(),
                "markov_safe": model.markov_safe,
                "metrics": model.metrics,
                "meta": model.meta,
                "saved_utc": utc_now(),
            },
            indent=2,
            default=str,
        )
    )
    return d


def load_model(settings: Settings, name: str, from_state: str) -> TransitionModel:
    d = model_dir(settings, name, from_state)
    if not (d / "model.txt").exists():
        raise FileNotFoundError(f"No model at {d}. Train it first.")
    payload = json.loads((d / "meta.json").read_text())
    return TransitionModel(
        from_state=payload["from_state"],
        booster=lgb.Booster(model_file=str(d / "model.txt")),
        features=payload["features"],
        label_map=payload["label_map"],
        encoder=CategoricalEncoder.from_dict(payload["encoder"]),
        markov_safe=payload["markov_safe"],
        metrics=payload["metrics"],
        meta=payload["meta"],
    )


def load_all(settings: Settings, name: str) -> dict[str, TransitionModel]:
    base = model_dir(settings, name)
    if not base.exists():
        raise FileNotFoundError(f"No model set {name!r} at {base}")
    out = {}
    for d in sorted(base.glob("FROM_STATE=*")):
        fs = d.name.split("=", 1)[1]
        out[fs] = load_model(settings, name, fs)
    return out


def save_registry_index(settings: Settings, name: str, payload: dict[str, Any]) -> Path:
    base = model_dir(settings, name)
    base.mkdir(parents=True, exist_ok=True)
    path = base / "registry.json"
    path.write_text(json.dumps({**payload, "saved_utc": utc_now()}, indent=2, default=str))
    return path


def read_registry_index(settings: Settings, name: str) -> dict[str, Any] | None:
    path = model_dir(settings, name) / "registry.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())
