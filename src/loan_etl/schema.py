"""Declarative schema registry: YAML field specs -> polars expressions.

This replaces the two hardcoded column lists and the positional ``cols_idx``
coercion in the previous loader. That approach had three failure modes this
module is built to eliminate:

1. The index list was authored 1-based and applied 0-based, so it converted the
   wrong columns (including the join key) and ran off the end of the frame.
   Here, casts are bound to field *names* resolved from the spec -- there is no
   positional arithmetic to get wrong.
2. Sentinels (9999 credit score, 999 DTI/LTV) were never mapped, so they entered
   models as real numbers. Here every sentinel is declared per field and nulled
   before the cast.
3. A field-count mismatch silently dropped every row. Here it raises.

Everything is read as Utf8 first and cast explicitly afterwards. That ordering
is deliberate: NET_SALE_PROCEEDS mixes numeric amounts with the literal codes
"C" and "U", so a reader-level numeric cast would destroy them irrecoverably.
"""

from __future__ import annotations

from dataclasses import dataclass, field as dc_field
from functools import lru_cache
from pathlib import Path
from typing import Any, Sequence

import polars as pl
import yaml

from .settings import SCHEMA_DIR

POLARS_DTYPES: dict[str, pl.DataType] = {
    "Utf8": pl.Utf8,
    "String": pl.Utf8,
    "Boolean": pl.Boolean,
    "Int8": pl.Int8,
    "Int16": pl.Int16,
    "Int32": pl.Int32,
    "Int64": pl.Int64,
    "Float32": pl.Float32,
    "Float64": pl.Float64,
}


class SchemaError(RuntimeError):
    """Raised when a schema spec is malformed or a file does not match it."""


@dataclass(frozen=True)
class Field:
    pos: int
    name: str
    dtype: str
    nulls: tuple[str, ...] = ()
    valid: dict[str, float] | None = None
    enum: tuple[str, ...] | None = None
    enum_strict: bool = True
    yyyymm: bool = False
    pattern: str | None = None
    categorical: bool = False
    special_codes: tuple[str, ...] = ()
    primary_key: bool = False
    foreign_key: str | None = None
    verify: bool = False
    desc: str = ""

    @property
    def polars_dtype(self) -> pl.DataType:
        try:
            return POLARS_DTYPES[self.dtype]
        except KeyError as exc:
            raise SchemaError(f"{self.name}: unknown dtype {self.dtype!r}") from exc

    @property
    def is_numeric(self) -> bool:
        return self.polars_dtype != pl.Utf8 and self.polars_dtype != pl.Boolean


@dataclass(frozen=True)
class TableSchema:
    version: int
    dataset: str
    field_count: int
    separator: str
    has_header: bool
    fields: tuple[Field, ...]
    source_path: Path | None = None
    _by_name: dict[str, Field] = dc_field(default_factory=dict, compare=False)

    # --- Lookup -------------------------------------------------------------
    @property
    def names(self) -> list[str]:
        return [f.name for f in self.fields]

    def __getitem__(self, name: str) -> Field:
        return self._by_name[name]

    def get(self, name: str) -> Field | None:
        return self._by_name.get(name)

    # --- Reading ------------------------------------------------------------
    def read_schema(self) -> dict[str, pl.DataType]:
        """All-Utf8 schema handed to ``scan_csv``. Casting happens after."""
        return {f.name: pl.Utf8 for f in self.fields}

    # --- Casting ------------------------------------------------------------
    def cast_exprs(self) -> list[pl.Expr]:
        return [_cast_expr(f) for f in self.fields]

    def derived_exprs(self) -> list[pl.Expr]:
        """Extra columns that do not replace a source field.

        - ``<NAME>_DT``: a real ``pl.Date`` for every YYYYMM field. The string
          form is retained because downstream code slices it; having both ends
          the dtype disagreement between the old loader and the notebooks.
        - ``NET_SALE_PROCEEDS_CODE`` / ``_AMT``: the C/U code and the numeric
          amount split apart.
        """
        out: list[pl.Expr] = []
        for f in self.fields:
            if f.yyyymm:
                out.append(
                    (pl.col(f.name) + pl.lit("01"))
                    .str.to_date("%Y%m%d", strict=False)
                    .alias(f"{f.name}_DT")
                )
            if f.special_codes:
                codes = list(f.special_codes)
                is_code = pl.col(f.name).is_in(codes)
                out.append(
                    pl.when(is_code)
                    .then(pl.col(f.name))
                    .otherwise(pl.lit(None, pl.Utf8))
                    .alias(f"{f.name}_CODE")
                )
                out.append(
                    pl.when(is_code)
                    .then(pl.lit(None, pl.Float64))
                    .otherwise(pl.col(f.name).cast(pl.Float64, strict=False))
                    .alias(f"{f.name}_AMT")
                )
        return out

    # --- Validation helpers -------------------------------------------------
    def range_violation_exprs(self) -> list[pl.Expr]:
        """Count values nulled by a *range* check rather than a declared sentinel.

        A spike here is the signal that an undocumented sentinel has appeared in
        a new release -- exactly the drift the old loader could not see.
        """
        out: list[pl.Expr] = []
        for f in self.fields:
            if not (f.valid and f.is_numeric):
                continue
            raw = _null_out(pl.col(f.name).str.strip_chars(), f.nulls).cast(
                f.polars_dtype, strict=False
            )
            pred = _range_predicate(raw, f.valid)
            out.append(
                (raw.is_not_null() & ~pred).sum().alias(f"range_violations__{f.name}")
            )
        return out

    def enum_violation_exprs(self) -> list[pl.Expr]:
        out: list[pl.Expr] = []
        for f in self.fields:
            if not f.enum:
                continue
            col = _null_out(pl.col(f.name).str.strip_chars(), f.nulls)
            out.append(
                (col.is_not_null() & ~col.is_in(list(f.enum)))
                .sum()
                .alias(f"enum_violations__{f.name}")
            )
        return out

    @property
    def primary_key(self) -> str | None:
        for f in self.fields:
            if f.primary_key:
                return f.name
        return None


# ---------------------------------------------------------------------------
# Expression builders
# ---------------------------------------------------------------------------


def _null_out(col: pl.Expr, values: Sequence[str]) -> pl.Expr:
    if not values:
        return col
    return (
        pl.when(col.is_in(list(values)))
        .then(pl.lit(None, pl.Utf8))
        .otherwise(col)
    )


def _range_predicate(col: pl.Expr, valid: dict[str, float]) -> pl.Expr:
    pred: pl.Expr | None = None
    if "min" in valid:
        pred = col >= valid["min"]
    if "max" in valid:
        upper = col <= valid["max"]
        pred = upper if pred is None else (pred & upper)
    if pred is None:
        return pl.lit(True)
    return pred


def _cast_expr(f: Field) -> pl.Expr:
    """strip -> null-out sentinels -> cast -> null-out out-of-range."""
    col = pl.col(f.name).str.strip_chars()
    col = _null_out(col, f.nulls)

    dtype = f.polars_dtype
    if dtype == pl.Utf8:
        return col.alias(f.name)

    col = col.cast(dtype, strict=False)
    if f.valid:
        col = (
            pl.when(_range_predicate(col, f.valid))
            .then(col)
            .otherwise(pl.lit(None, dtype))
        )
    return col.alias(f.name)


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def _parse_field(raw: dict[str, Any]) -> Field:
    return Field(
        pos=raw["pos"],
        name=raw["name"],
        dtype=raw["dtype"],
        nulls=tuple(raw.get("nulls", ()) or ()),
        valid=raw.get("valid"),
        enum=tuple(raw["enum"]) if raw.get("enum") else None,
        enum_strict=bool(raw.get("enum_strict", True)),
        yyyymm=bool(raw.get("yyyymm", False)),
        pattern=raw.get("pattern"),
        categorical=bool(raw.get("categorical", False)),
        special_codes=tuple(raw.get("special_codes", ()) or ()),
        primary_key=bool(raw.get("primary_key", False)),
        foreign_key=raw.get("foreign_key"),
        verify=bool(raw.get("verify", False)),
        desc=(raw.get("desc") or "").strip(),
    )


@lru_cache(maxsize=8)
def load_schema(
    dataset: str, version: int = 47, schema_dir: Path | None = None
) -> TableSchema:
    """Load ``{dataset}_v{version}.yaml`` from the schema directory."""
    directory = Path(schema_dir) if schema_dir else SCHEMA_DIR
    path = directory / f"{dataset}_v{version}.yaml"
    if not path.exists():
        available = sorted(p.name for p in directory.glob("*_v*.yaml"))
        raise SchemaError(f"No schema at {path}. Available: {available}")

    spec = yaml.safe_load(path.read_text())
    fields = tuple(_parse_field(f) for f in spec["fields"])

    positions = [f.pos for f in fields]
    if positions != list(range(1, len(fields) + 1)):
        raise SchemaError(
            f"{path.name}: field positions must be contiguous 1..N, got {positions}"
        )
    declared = spec["field_count"]
    if declared != len(fields):
        raise SchemaError(
            f"{path.name}: field_count={declared} but {len(fields)} fields defined"
        )
    names = [f.name for f in fields]
    if len(set(names)) != len(names):
        dupes = sorted({n for n in names if names.count(n) > 1})
        raise SchemaError(f"{path.name}: duplicate field names {dupes}")

    return TableSchema(
        version=spec["version"],
        dataset=spec["dataset"],
        field_count=declared,
        separator=spec.get("separator", "|"),
        has_header=bool(spec.get("has_header", False)),
        fields=fields,
        source_path=path,
        _by_name={f.name: f for f in fields},
    )


def check_arity(path: Path, schema: TableSchema, sample_lines: int = 200) -> int:
    """Pre-flight: confirm the file's field count matches the schema.

    Raises instead of silently skipping. The previous loader compared the field
    count per line and dropped non-matching rows with a print statement, which
    meant a layout change wiped out an entire vintage without failing.
    """
    sep = schema.separator
    counts: set[int] = set()
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        for i, line in enumerate(fh):
            if i >= sample_lines:
                break
            line = line.rstrip("\r\n")
            if not line:
                continue
            counts.add(line.count(sep) + 1)

    if not counts:
        raise SchemaError(f"{path} is empty or contains no parsable lines")
    if counts != {schema.field_count}:
        raise SchemaError(
            f"{path.name}: expected {schema.field_count} fields "
            f"(schema {schema.dataset} v{schema.version}) but the first "
            f"{sample_lines} lines have field counts {sorted(counts)}. "
            "This is a layout-version mismatch -- add the correct "
            f"config/schemas/{schema.dataset}_v<N>.yaml rather than dropping rows."
        )
    return schema.field_count