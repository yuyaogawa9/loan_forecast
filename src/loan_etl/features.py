"""Safe feature selection for the loan-month panel.

The panel intentionally retains every column, including ones populated only at
termination. That is correct for auditing and for building loss targets, but it
means the raw frame must never be handed to a model as-is. This module is the
gate between the two:

    from loan_etl.features import training_frame
    lf, cols = training_frame(panel, target="default")

`cols.features` contains only columns knowable at the start of the reporting
month. Nothing classified `target` or `leakage` can reach a model through this
path.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Sequence

import polars as pl
import yaml

from .settings import SCHEMA_DIR

FEATURE_ROLES = ("feature_static", "feature_dynamic", "feature_macro")
UNCLASSIFIED = "unclassified"


class FeatureError(RuntimeError):
    pass


@dataclass(frozen=True)
class RoleSpec:
    exact: frozenset[str]
    patterns: tuple[re.Pattern[str], ...]

    def matches(self, column: str) -> bool:
        return column in self.exact or any(p.search(column) for p in self.patterns)


@dataclass(frozen=True)
class ColumnRegistry:
    precedence: tuple[str, ...]
    roles: dict[str, RoleSpec]
    target_groups: dict[str, str]
    categorical_features: frozenset[str]

    def role_of(self, column: str) -> str:
        for role in self.precedence:
            spec = self.roles.get(role)
            if spec and spec.matches(column):
                return role
        return UNCLASSIFIED

    def classify(self, columns: Iterable[str]) -> dict[str, list[str]]:
        out: dict[str, list[str]] = {r: [] for r in (*self.precedence, UNCLASSIFIED)}
        for c in columns:
            out[self.role_of(c)].append(c)
        return out

    def resolve_target(self, target: str) -> str:
        """Accept either a group alias ('default') or a column name."""
        if target in self.target_groups:
            return self.target_groups[target]
        if self.role_of(target) == "target":
            return target
        raise FeatureError(
            f"Unknown target {target!r}. Use a column with role 'target', or one "
            f"of: {sorted(self.target_groups)}"
        )


@dataclass(frozen=True)
class SelectedColumns:
    target: str
    features: tuple[str, ...]
    identifiers: tuple[str, ...]
    categorical: tuple[str, ...]
    excluded_leakage: tuple[str, ...] = field(default=())
    dropped_degenerate: tuple[str, ...] = field(default=())

    def __len__(self) -> int:
        return len(self.features)


@lru_cache(maxsize=4)
def load_registry(schema_dir: Path | None = None) -> ColumnRegistry:
    directory = Path(schema_dir) if schema_dir else SCHEMA_DIR
    spec = yaml.safe_load((directory / "columns.yaml").read_text())

    roles: dict[str, RoleSpec] = {}
    for name, body in (spec.get("roles") or {}).items():
        body = body or {}
        roles[name] = RoleSpec(
            exact=frozenset(body.get("exact") or ()),
            patterns=tuple(re.compile(p) for p in (body.get("patterns") or ())),
        )

    precedence = tuple(spec["precedence"])
    missing = set(roles) - set(precedence)
    if missing:
        raise FeatureError(f"roles not listed in precedence: {sorted(missing)}")

    return ColumnRegistry(
        precedence=precedence,
        roles=roles,
        target_groups=dict(spec.get("target_groups") or {}),
        categorical_features=frozenset(spec.get("categorical_features") or ()),
    )


def unclassified_columns(columns: Iterable[str], schema_dir: Path | None = None) -> list[str]:
    """Panel columns the registry does not describe.

    Non-empty means columns.yaml has drifted behind the panel. Treated as a hard
    failure so an unreviewed column can never silently become a feature.
    """
    reg = load_registry(schema_dir)
    return sorted(c for c in columns if reg.role_of(c) == UNCLASSIFIED)


def select(
    columns: Sequence[str],
    target: str,
    *,
    include_macro: bool = True,
    include_static: bool = True,
    include_dynamic: bool = True,
    drop: Sequence[str] = (),
    schema_dir: Path | None = None,
) -> SelectedColumns:
    """Resolve the feature/target split for a panel with these columns."""
    reg = load_registry(schema_dir)

    stray = unclassified_columns(columns, schema_dir)
    if stray:
        raise FeatureError(
            f"{len(stray)} column(s) have no role in columns.yaml: {stray}. "
            "Classify them before training -- an unreviewed column could be leakage."
        )

    target_col = reg.resolve_target(target)
    if target_col not in columns:
        raise FeatureError(f"Target {target_col!r} is not in the panel.")

    wanted = set()
    if include_static:
        wanted.add("feature_static")
    if include_dynamic:
        wanted.add("feature_dynamic")
    if include_macro:
        wanted.add("feature_macro")

    by_role = reg.classify(columns)
    dropped = set(drop)
    features = tuple(
        c
        for role in FEATURE_ROLES
        if role in wanted
        for c in by_role[role]
        if c not in dropped
    )
    identifiers = tuple(by_role["identifier"])
    categorical = tuple(c for c in features if c in reg.categorical_features)

    return SelectedColumns(
        target=target_col,
        features=features,
        identifiers=identifiers,
        categorical=categorical,
        excluded_leakage=tuple(by_role["leakage"]),
    )


def training_frame(
    lf: pl.LazyFrame,
    target: str,
    *,
    modelable_only: bool = True,
    include_macro: bool = True,
    include_static: bool = True,
    include_dynamic: bool = True,
    drop: Sequence[str] = (),
    schema_dir: Path | None = None,
    encode_categorical: bool = True,
    drop_degenerate: bool = True,
) -> tuple[pl.LazyFrame, SelectedColumns]:
    """Project a panel down to identifiers + safe features + one target.

    ``modelable_only`` drops each loan's first observation, which has no lagged
    state by construction and would otherwise contribute a row of nulls.
    ``encode_categorical`` casts string features to ``pl.Categorical`` so the
    frame is directly consumable by XGBoost with ``enable_categorical=True``.
    """
    columns = lf.collect_schema().names()
    cols = select(
        columns,
        target,
        include_macro=include_macro,
        include_static=include_static,
        include_dynamic=include_dynamic,
        drop=drop,
        schema_dir=schema_dir,
    )

    if modelable_only and "IS_MODELABLE" in columns:
        lf = lf.filter(pl.col("IS_MODELABLE"))

    keep = [*cols.identifiers, *cols.features, cols.target]
    # dict.fromkeys preserves order while removing any accidental duplicate.
    lf = lf.select(list(dict.fromkeys(keep)))

    # The categorical set is derived from actual dtypes, not only the YAML list.
    # A hand-maintained list silently misses new string columns, and a single
    # leftover string makes XGBoost reject the whole frame -- so any Utf8
    # feature is categorical by definition, and the YAML adds numeric-coded
    # columns that should be treated as categories anyway.
    features = cols.features
    dropped: tuple[str, ...] = ()
    if drop_degenerate:
        degenerate = find_degenerate(lf, features)
        if degenerate:
            dropped = tuple(degenerate)
            features = tuple(f for f in features if f not in degenerate)
            lf = lf.drop(degenerate)

    schema = lf.collect_schema()
    string_features = {c for c in features if schema.get(c) == pl.Utf8}
    categorical = tuple(
        c for c in features if c in string_features or c in cols.categorical
    )
    cols = replace(
        cols, features=features, categorical=categorical, dropped_degenerate=dropped
    )

    if encode_categorical and categorical:
        lf = encode_categoricals(lf, categorical)
    return lf, cols


def find_degenerate(lf: pl.LazyFrame, features: Sequence[str]) -> list[str]:
    """Features carrying no information: entirely null, or a single constant.

    Worth removing rather than tolerating. An all-null *categorical* has zero
    levels, and XGBoost's categorical path raises on that rather than ignoring
    it -- so one such column blocks the whole fit. This is not hypothetical on
    real data: fields added in later releases (PROPERTY_VALUATION_METHOD,
    SUPER_CONFORMING_FLAG) are null for every loan in early vintages.

    Costs one pass over the frame, so pass ``drop_degenerate=False`` if you are
    selecting from a very large panel and already know the columns are sound.
    """
    present = [f for f in features if f in lf.collect_schema().names()]
    if not present:
        return []
    stats = lf.select(
        [pl.col(c).drop_nulls().n_unique().alias(c) for c in present]
    ).collect().to_dicts()[0]
    return [c for c, n in stats.items() if (n or 0) <= 1]


def encode_categoricals(lf: pl.LazyFrame, columns: Sequence[str]) -> pl.LazyFrame:
    """Cast string features to ``pl.Categorical``.

    XGBoost cannot consume raw strings. Categorical dtype survives the polars ->
    pandas -> XGBoost hop, which lets you fit with ``enable_categorical=True``
    and skip one-hot encoding entirely -- worth doing here, because SELLER_NAME
    and POSTAL_CODE have enough levels that one-hot would be unworkable.
    """
    present = [c for c in columns if c in lf.collect_schema().names()]
    return lf.with_columns([pl.col(c).cast(pl.Categorical) for c in present])


def starting_state(lf: pl.LazyFrame, state: str | Sequence[str]) -> pl.LazyFrame:
    """Restrict to loan-months that began in a given delinquency state.

    Worth understanding before you fit: prior delinquency is by far the
    strongest predictor of current delinquency, because delinquency persists. A
    model trained across all starting states will post a high AUC that mostly
    reflects that persistence rather than any credit-risk signal.

    The usual remedy is to condition on where the loan started -- e.g. fit
    "becomes 30 days down" only on loans that were current last month:

        new_dlq = starting_state(panel, "0")

    ``PRIOR_DLQ_STATUS`` uses Freddie's raw codes: "0" current, "1".."N" months
    delinquent, "RA" in REO acquisition.
    """
    states = [state] if isinstance(state, str) else list(state)
    return lf.filter(pl.col("PRIOR_DLQ_STATUS").is_in(states))
