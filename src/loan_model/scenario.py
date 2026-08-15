"""Macro paths for forward-looking work.

Two shapes, because projection and simulation need different things:

``Scenario``   -- one macro row per period, for a single loan or a homogeneous
                  cohort. What the matrix projection consumes.
``MacroPanel`` -- the full point-in-time panel, resolvable per (period, state) in
                  one batched join. What simulation consumes, because a portfolio
                  spans all 52 states at once.

The distinction is not cosmetic. State-level series are not comparable across
states -- California's HPI index sat near 643 in 2007 while Texas was near 224 --
so collapsing them with an aggregate function and handing every loan the result
silently mis-marks equity for most of the book. `scenario_from_macro_panel`
guards that by requiring an explicit ``state``; `MacroPanel` removes the need to
choose by keeping all of them and joining on the loan's own state.
"""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

import polars as pl

PERIOD_COLUMN = "MONTHLY_REPORTING_PERIOD"
DEFAULT_PERIOD_KEY = "AVAILABLE_PERIOD"


@dataclass
class Scenario:
    """Macro path indexed by reporting period (YYYYMM)."""

    frame: pl.DataFrame
    period_column: str = PERIOD_COLUMN

    def row_for(self, period: str) -> dict[str, Any]:
        hit = self.frame.filter(pl.col(self.period_column) == period)
        if hit.height == 0:
            # Hold the last known value rather than injecting nulls, which would
            # silently blank every macro feature for the rest of the horizon.
            hit = self.frame.tail(1)
        return hit.to_dicts()[0] if hit.height else {}

    @property
    def macro_columns(self) -> list[str]:
        return [c for c in self.frame.columns if c != self.period_column]


def scenario_from_macro_panel(
    macro_path,
    columns: Sequence[str],
    period_column: str = DEFAULT_PERIOD_KEY,
    *,
    state: str | None = None,
) -> Scenario:
    """Build a baseline scenario from the realised macro panel.

    Useful for backtesting: projecting a historical vintage under the macro path
    that actually occurred isolates model error from scenario error.

    ``state`` is required whenever state-level series are in play. Pivoting the
    whole panel without it collapses all 52 states into a single column via the
    aggregate function, so a California loan would be projected against whatever
    state happened to sort last -- silently wrong, and badly so for HPI.
    """
    panel = pl.read_parquet(macro_path)
    national = panel.filter(pl.col("GEO_LEVEL") == "national")
    if state is not None:
        local = panel.filter(
            (pl.col("GEO_LEVEL") == "state") & (pl.col("GEO_CODE") == state)
        )
        panel = pl.concat([national, local], how="vertical_relaxed")
    else:
        panel = national

    wide = panel.pivot(
        on="SERIES_NAME", index=period_column, values="VALUE", aggregate_function="last"
    ).rename({period_column: PERIOD_COLUMN})
    keep = [PERIOD_COLUMN] + [c for c in columns if c in wide.columns]
    return Scenario(wide.select(keep).sort(PERIOD_COLUMN))


@dataclass
class MacroPanel:
    """Point-in-time macro, resolvable per (period, state) in one join.

    Held wide and pre-sliced by period so a simulation month costs one small
    join of N loan rows against ~52 state rows, rather than a filter over the
    whole panel per loan.
    """

    national: pl.DataFrame
    state: pl.DataFrame
    _periods: list[str] = field(default_factory=list, repr=False)
    _national_rows: dict[str, dict[str, Any]] = field(default_factory=dict, repr=False)
    _state_slices: dict[str, pl.DataFrame] = field(default_factory=dict, repr=False)

    STATE_KEY = "GEO_CODE"

    @classmethod
    def load(
        cls,
        macro_path: Path | str,
        *,
        include_pit_unsafe: bool = False,
        period_column: str = DEFAULT_PERIOD_KEY,
    ) -> "MacroPanel":
        panel = pl.read_parquet(macro_path)
        if not include_pit_unsafe and "PIT_UNSAFE" in panel.columns:
            panel = panel.filter(~pl.col("PIT_UNSAFE"))

        def relabel(frame: pl.DataFrame) -> pl.DataFrame:
            if period_column in frame.columns and period_column != PERIOD_COLUMN:
                return frame.rename({period_column: PERIOD_COLUMN})
            return frame

        return cls.build(
            relabel(cls._pivot(panel, "national", [period_column])),
            relabel(cls._pivot(panel, "state", [period_column, cls.STATE_KEY])),
        )

    @staticmethod
    def _pivot(panel: pl.DataFrame, level: str, index: list[str]) -> pl.DataFrame:
        sub = panel.filter(pl.col("GEO_LEVEL") == level)
        if sub.height == 0:
            return pl.DataFrame({c: [] for c in index}, schema={c: pl.Utf8 for c in index})
        return sub.pivot(
            on="SERIES_NAME", index=index, values="VALUE", aggregate_function="last"
        )

    @classmethod
    def build(cls, national: pl.DataFrame, state: pl.DataFrame) -> "MacroPanel":
        national = national.sort(PERIOD_COLUMN)
        periods = national[PERIOD_COLUMN].to_list() if national.height else []
        rows = {p: r for p, r in zip(periods, national.to_dicts())}

        slices: dict[str, pl.DataFrame] = {}
        if state.height:
            for (period,), part in state.partition_by(
                PERIOD_COLUMN, as_dict=True, include_key=False
            ).items():
                slices[period] = part
        return cls(
            national=national,
            state=state,
            _periods=periods,
            _national_rows=rows,
            _state_slices=slices,
        )

    # --- lookup -----------------------------------------------------------
    @property
    def columns(self) -> list[str]:
        nat = [c for c in self.national.columns if c != PERIOD_COLUMN]
        st = [c for c in self.state.columns if c not in (PERIOD_COLUMN, self.STATE_KEY)]
        return list(dict.fromkeys(nat + st))

    def effective_period(self, period: str) -> str | None:
        """The latest published period at or before ``period``.

        Holding the last known value past the end of the panel is deliberate: a
        forecast horizon routinely runs beyond the macro history, and injecting
        nulls there would blank every macro feature for the remainder of the run.
        """
        if not self._periods:
            return None
        if period in self._national_rows:
            return period
        i = bisect_right(self._periods, period)
        return self._periods[i - 1] if i else self._periods[0]

    def attach(
        self,
        frame: pl.DataFrame,
        period: str,
        *,
        state_column: str = "PROPERTY_STATE",
        columns: Sequence[str] | None = None,
    ) -> pl.DataFrame:
        """Overwrite ``frame``'s macro columns with this period's values.

        National series broadcast as literals; state series join on the loan's
        own ``PROPERTY_STATE``. Both replace whatever the frame already held, so
        a simulation stepping forward re-marks the whole book each month.
        """
        effective = self.effective_period(period)
        if effective is None:
            return frame
        wanted = set(columns) if columns is not None else None

        row = self._national_rows.get(effective, {})
        literals = [
            pl.lit(v).alias(k)
            for k, v in row.items()
            if k != PERIOD_COLUMN and (wanted is None or k in wanted)
        ]
        if literals:
            frame = frame.with_columns(literals)

        part = self._state_slices.get(effective)
        if part is not None and part.height and state_column in frame.columns:
            keep = [
                c
                for c in part.columns
                if c != self.STATE_KEY and (wanted is None or c in wanted)
            ]
            if keep:
                # Drop the incumbent columns first: a join would otherwise suffix
                # them and the model would keep reading the stale originals.
                frame = frame.drop([c for c in keep if c in frame.columns])
                frame = frame.join(
                    part.select(self.STATE_KEY, *keep),
                    left_on=state_column,
                    right_on=self.STATE_KEY,
                    how="left",
                )
        return frame
