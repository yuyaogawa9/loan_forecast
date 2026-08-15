"""polars -> Arrow -> LightGBM, with pandas never involved.

This is the direct fix for the RAM problem. The previous path went
``df.select(features).to_pandas()``, which materialises a full copy; at 73.6M x
60 float64 that is ~35 GB. LightGBM ingests a ``pyarrow.Table`` natively, so the
frame goes straight from polars to the training buffer.

Two constraints discovered by testing rather than assuming:

1. **LightGBM rejects dictionary-encoded Arrow columns** -- it raises
   ``Arrow table may only have integer or floating point datatypes``. Categorical
   features must be integer codes before ``to_arrow()``.

2. **polars categorical codes are frame-local.** ``to_physical()`` on a
   Categorical numbers the levels in order of appearance *within that frame*, so
   encoding train and validation separately would assign different codes to the
   same state and silently corrupt the model. The encoding is therefore fitted
   once, persisted with the model, and reapplied verbatim at predict time.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Sequence

import lightgbm as lgb
import polars as pl
import pyarrow as pa

MISSING_CODE = -1  # LightGBM reads negative categorical codes as missing


class DatasetError(RuntimeError):
    pass


@dataclass
class CategoricalEncoder:
    """Explicit, persistable string -> integer-code mapping.

    Not a convenience wrapper around ``to_physical()``: an explicit mapping is
    the only way to guarantee the same level gets the same code in training, in
    validation, and months later at prediction time.
    """

    mapping: dict[str, dict[str, int]] = field(default_factory=dict)

    @classmethod
    def fit(cls, df: pl.DataFrame, columns: Sequence[str]) -> "CategoricalEncoder":
        mapping: dict[str, dict[str, int]] = {}
        for c in columns:
            levels = (
                df.select(pl.col(c).cast(pl.Utf8))
                .drop_nulls()
                .unique()
                .sort(c)[c]
                .to_list()
            )
            mapping[c] = {v: i for i, v in enumerate(levels)}
        return cls(mapping=mapping)

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        exprs = []
        for c, m in self.mapping.items():
            if c not in df.columns:
                continue
            exprs.append(
                pl.col(c)
                .cast(pl.Utf8)
                .replace_strict(m, default=MISSING_CODE, return_dtype=pl.Int32)
                .alias(c)
            )
        return df.with_columns(exprs) if exprs else df

    def to_dict(self) -> dict[str, Any]:
        return {"mapping": self.mapping}

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "CategoricalEncoder":
        return cls(mapping={k: dict(v) for k, v in payload["mapping"].items()})

    @property
    def columns(self) -> list[str]:
        return list(self.mapping)


def resolve_categoricals(
    df: pl.DataFrame, features: Sequence[str], hint: Sequence[str] = ()
) -> list[str]:
    """Every feature that must be integer-encoded, derived from dtype.

    Dtype is the authority, not a hand-maintained list. The YAML list once
    missed four string columns, and a single un-encoded string makes LightGBM
    reject the whole frame -- so anything Utf8/Categorical/Enum is categorical
    whether or not somebody remembered to declare it. ``hint`` adds columns that
    are stored numerically but should still be treated as unordered.
    """
    schema = df.schema
    out = [
        c
        for c in features
        if schema.get(c) in (pl.Utf8, pl.Categorical, pl.Enum) or c in set(hint)
    ]
    return list(dict.fromkeys(out))


def to_arrow_features(
    df: pl.DataFrame,
    features: Sequence[str],
    encoder: CategoricalEncoder | None = None,
) -> pa.Table:
    """Feature matrix as an Arrow table: categoricals as ints, rest float32.

    float32 halves memory versus float64 and costs nothing -- LightGBM bins
    features to at most 255 values anyway.
    """
    frame = encoder.transform(df) if encoder else df
    cat = set(encoder.columns) if encoder else set()

    # Catch un-encoded categorical/string columns here rather than letting the
    # cast fail. polars raises "cannot cast categorical types to Float32", which
    # says nothing about the actual fix -- that the column needs an encoder.
    schema = frame.schema
    unencoded = [
        c
        for c in features
        if c not in cat and schema.get(c) in (pl.Categorical, pl.Enum, pl.Utf8)
    ]
    if unencoded:
        raise DatasetError(
            f"dictionary-encoded or string columns reached Arrow without an "
            f"encoding: {unencoded}. LightGBM accepts only numeric Arrow types; "
            "fit a CategoricalEncoder over them first."
        )

    exprs = []
    for c in features:
        if c in cat:
            exprs.append(pl.col(c).cast(pl.Int32))
        else:
            exprs.append(pl.col(c).cast(pl.Float32, strict=False))
    table = frame.select(exprs).to_arrow()

    bad = [f.name for f in table.schema if pa.types.is_dictionary(f.type)]
    if bad:
        raise DatasetError(
            f"dictionary-encoded columns reached Arrow: {bad}. LightGBM cannot "
            "consume them; they must be integer-encoded first."
        )
    return table


def build_dataset(
    df: pl.DataFrame,
    features: Sequence[str],
    label_column: str,
    *,
    encoder: CategoricalEncoder | None = None,
    weight_column: str | None = None,
    reference: lgb.Dataset | None = None,
    free_raw_data: bool = True,
    label_dtype: pl.DataType | None = None,
) -> lgb.Dataset:
    """Construct an ``lgb.Dataset`` without ever touching pandas.

    ``label_dtype`` defaults to Int32 for the class-index labels the transition
    multinomials use, and MUST be set to a float type for regression targets.
    Leaving it integral silently truncates: a loss severity of 0.453 becomes 0,
    and the fitted model then describes a variable that is zero almost
    everywhere -- which looks like a fit, not like an error.
    """
    table = to_arrow_features(df, features, encoder)
    label = df[label_column].cast(label_dtype or pl.Int32).to_arrow()
    weight = (
        df[weight_column].cast(pl.Float32).to_arrow() if weight_column else None
    )
    categorical = [c for c in (encoder.columns if encoder else []) if c in features]

    return lgb.Dataset(
        table,
        label=label,
        weight=weight,
        feature_name=list(features),
        categorical_feature=categorical or "auto",
        reference=reference,
        free_raw_data=free_raw_data,
    )


def estimate_memory_gb(n_rows: int, n_features: int) -> float:
    """Feature-matrix footprint in float32, for sizing before committing."""
    return n_rows * n_features * 4 / (1024**3)
