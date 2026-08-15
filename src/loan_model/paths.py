"""Per-path delinquency history, reproduced exactly as the ETL derives it.

This is the correctness core of Monte Carlo simulation, and it is kept separate
from the sampler so it can be tested against real panel data on its own.

The matrix projection cannot carry history: at step ``t`` a loan is spread across
states rather than sitting on one path, so "how long has THIS loan been
delinquent" has no value. That is why `features.select` has a `markov_safe_only`
switch and why seven columns are excluded by it. Simulation removes the
restriction -- each path IS a single history -- but only if the history is
reconstructed the way `with_panel_state` built it during training. A simulator
that computes these features even slightly differently feeds the model a
covariate distribution it has never seen, and the resulting probabilities are
wrong in a way nothing downstream can detect.

So every rule below is a transcription of
:func:`loan_etl.derive.events.with_panel_state`, including two behaviours that
look like defects and are deliberately preserved:

1. **REO months are not delinquent months.** The ETL derives ``DLQ_MONTHS`` by
   casting ``CURRENT_LOAN_DELINQUENCY_STATUS``, and "RA" does not parse, so it
   becomes null. ``is_dlq`` is then False, which means an REO month contributes
   nothing to ``N_DLQ_MONTHS_TO_DATE`` and -- because the spell counter is
   ``(~is_dlq).cum_sum()`` -- actually *breaks* the delinquency run.

2. **90+ is a lumped state but the month count is not.** ``EVENT`` collapses
   everything at or beyond three months into ``DLQ_90_PLUS``, yet the underlying
   ``DLQ_MONTHS`` keeps counting: 3, 4, 5, ... Preserving that counter is the
   whole point of simulating. It is precisely what a first-order chain destroys,
   and it drives the duration dependence that makes cure rates fall from 51.4%
   at one month down to 13.3% at seven or more.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

import numpy as np
import polars as pl

from .states import TransitionConfig
from .transition import StateSpace

# ``DLQ_MONTHS`` is null whenever the raw status does not parse as a number --
# in practice "RA", a loan in REO acquisition. Carried as a negative sentinel so
# the history stays in a single integer array, and rendered back to a real null
# at the frame boundary.
DLQ_NULL = -1

# A destination that says nothing about delinquency (any absorbing state): keep
# whatever the path already had, since it is leaving the book this month.
DLQ_CARRY = -2

MAX_STATUS_CODE = 99
REO_STATUS = "RA"

# Vectorised int -> zero-padded code lookup. Freddie's status field is a
# two-character string and the fitted encoders carry levels "00".."99", so a
# path delinquent beyond 99 months clamps rather than falling off the alphabet
# into MISSING_CODE.
_STATUS_CODES = np.array([f"{i:02d}" for i in range(MAX_STATUS_CODE + 1)], dtype=object)

PATH_FEATURES = (
    "PRIOR_DLQ_STATUS",
    "PRIOR_DLQ_MONTHS",
    "MAX_DLQ_MONTHS_TO_DATE",
    "N_DLQ_MONTHS_TO_DATE",
    "PRIOR_DLQ_RUN_LENGTH",
    "MONTHS_SINCE_FIRST_DLQ",
    "EVER_DLQ_30_TO_DATE",
    "EVER_DLQ_90_TO_DATE",
    "PRIOR_IS_MODIFIED",
)


def status_strings(dlq_months: np.ndarray) -> np.ndarray:
    """Delinquency month count -> Freddie's raw status code.

    The inverse of the ETL's ``status.cast(Int16, strict=False)``. Getting this
    wrong is not hypothetical: the matrix projection feeds the pooled
    deep-delinquency model the literal bucket label "03_PLUS", which is not a
    level any encoder was fitted on, so it lands on MISSING_CODE and silently
    deletes the strongest predictor in the state where charge-offs originate.
    """
    codes = _STATUS_CODES[np.clip(dlq_months, 0, MAX_STATUS_CODE)]
    return np.where(dlq_months < 0, REO_STATUS, codes)


def period_index(periods: pl.Series | Sequence[str]) -> np.ndarray:
    """YYYYMM -> a monotone month counter.

    The clock for every history feature. Reporting periods are monotonic;
    LOAN_AGE is not, because a modification resets it.
    """
    s = periods if isinstance(periods, pl.Series) else pl.Series(list(periods))
    y = s.str.slice(0, 4).cast(pl.Int32)
    m = s.str.slice(4, 2).cast(pl.Int32)
    return (y * 12 + m).to_numpy().astype(np.int32)


def _nullable_int(values: np.ndarray, null_mask: np.ndarray) -> pl.Series:
    s = pl.Series(values.astype(np.int32), dtype=pl.Int32)
    idx = np.flatnonzero(null_mask)
    return s.scatter(idx, None) if idx.size else s


@dataclass(frozen=True)
class DlqCoding:
    """State index -> delinquency month count, derived from the config.

    Not a hardcoded name list. ``model_key_for_state`` is already the single
    bridge between named states and Freddie's numeric codes, so the coding is
    read back out of it and stays correct if the state space is re-cut.
    """

    fixed: np.ndarray   # per state index: month count, DLQ_NULL, or DLQ_CARRY
    deep: np.ndarray    # per state index: True where the count keeps climbing

    @classmethod
    def from_config(cls, space: StateSpace, cfg: TransitionConfig) -> "DlqCoding":
        fixed = np.full(space.size, DLQ_CARRY, dtype=np.int32)
        deep = np.zeros(space.size, dtype=bool)
        deep_label = cfg.from_state["deep_delinquency_label"]
        reo_code = cfg.from_state["reo_code"]

        for state in space.states:
            i = space.index(state)
            key = cfg.model_key_for_state(state)
            if key == deep_label:
                deep[i] = True
            elif key == reo_code:
                fixed[i] = DLQ_NULL
            else:
                try:
                    fixed[i] = int(key)
                except ValueError:
                    pass  # absorbing: carry
        return cls(fixed=fixed, deep=deep)

    def next_months(self, dest: np.ndarray, previous: np.ndarray) -> np.ndarray:
        """Delinquency count after landing in ``dest``, given the prior count."""
        out = self.fixed[dest]
        # Entering 90+ from 60 starts the counter at the collapse threshold;
        # staying in 90+ advances it. Without this the count would pin at 3 and
        # a loan twenty-four months down would look freshly ninety days late.
        climbing = np.where(previous >= 3, previous + 1, 3)
        out = np.where(self.deep[dest], climbing, out)
        return np.where(out == DLQ_CARRY, previous, out).astype(np.int32)


@dataclass
class PathHistory:
    """Delinquency history for N paths, current through the last closed month.

    Arrays are parallel and full-length; the simulator advances them in place
    rather than allocating per month. All of it is O(N) vectorised work -- there
    is deliberately no per-path Python loop anywhere in this module.
    """

    dlq_months: np.ndarray      # count at the end of the last closed month
    run_length: np.ndarray      # consecutive delinquent months ending there
    max_dlq: np.ndarray         # running maximum
    n_dlq: np.ndarray           # count of delinquent months so far
    first_dlq_month: np.ndarray  # month index of first delinquency, -1 if never
    is_modified: np.ndarray     # last known modification flag

    @classmethod
    def fresh(cls, n: int) -> "PathHistory":
        """A cohort observed from origination: no history by construction."""
        z = lambda: np.zeros(n, dtype=np.int32)  # noqa: E731
        return cls(
            dlq_months=z(),
            run_length=z(),
            max_dlq=z(),
            n_dlq=z(),
            first_dlq_month=np.full(n, -1, dtype=np.int32),
            is_modified=np.zeros(n, dtype=bool),
        )

    @classmethod
    def from_panel(cls, df: pl.DataFrame) -> "PathHistory":
        """Seed from real panel rows, for simulations starting mid-life.

        A live portfolio at month zero is not a fresh cohort: loans carry
        accumulated delinquency history, and starting them at zero would tell the
        model every one of them had a clean record.
        """
        n = df.height

        def ints(name: str, default: int = 0) -> np.ndarray:
            if name not in df.columns:
                return np.full(n, default, dtype=np.int32)
            return df[name].cast(pl.Int32).fill_null(default).to_numpy().astype(np.int32)

        def present(name: str) -> np.ndarray:
            # Null-detection has to happen on the polars side. `to_numpy()` on a
            # nullable integer column yields float64 with NaN, and NaN is not
            # null to polars -- testing for it afterwards silently returns False
            # everywhere and every loan looks like it has been delinquent.
            if name not in df.columns:
                return np.zeros(n, dtype=bool)
            return df[name].is_not_null().to_numpy()

        month = (
            period_index(df["MONTHLY_REPORTING_PERIOD"])
            if "MONTHLY_REPORTING_PERIOD" in df.columns
            else ints("LOAN_AGE")
        )
        since, known = ints("MONTHS_SINCE_FIRST_DLQ"), present("MONTHS_SINCE_FIRST_DLQ")

        return cls(
            dlq_months=ints("PRIOR_DLQ_MONTHS", DLQ_NULL),
            run_length=ints("PRIOR_DLQ_RUN_LENGTH"),
            max_dlq=ints("MAX_DLQ_MONTHS_TO_DATE"),
            n_dlq=ints("N_DLQ_MONTHS_TO_DATE"),
            first_dlq_month=np.where(known, month - since, -1).astype(np.int32),
            is_modified=(
                df["PRIOR_IS_MODIFIED"].fill_null(False).to_numpy().astype(bool)
                if "PRIOR_IS_MODIFIED" in df.columns
                else np.zeros(n, dtype=bool)
            ),
        )

    def __len__(self) -> int:
        return len(self.dlq_months)

    def take(self, idx: np.ndarray) -> "PathHistory":
        return PathHistory(
            dlq_months=self.dlq_months[idx],
            run_length=self.run_length[idx],
            max_dlq=self.max_dlq[idx],
            n_dlq=self.n_dlq[idx],
            first_dlq_month=self.first_dlq_month[idx],
            is_modified=self.is_modified[idx],
        )

    # --- rendering --------------------------------------------------------
    def features(self, month: np.ndarray) -> dict[str, Any]:
        """The PRIOR_* view used to predict the month at index ``month``.

        Everything here is knowable at the START of that month, matching the
        contract `with_panel_state` establishes for the training panel.

        ``month`` is a monotone reporting-period counter, not LOAN_AGE. The ETL
        used to rank on LOAN_AGE, which a modification resets, and that let a
        future delinquency read as a past one -- see the note in
        `events.with_panel_state`.
        """
        dlq = self.dlq_months
        is_null = dlq < 0

        # Null until the loan's first delinquency lies STRICTLY in the past;
        # without the guard the feature would read the very event being predicted.
        known = (self.first_dlq_month >= 0) & (self.first_dlq_month < month)
        since = np.where(known, month - self.first_dlq_month, 0)

        return {
            "PRIOR_DLQ_STATUS": pl.Series(status_strings(dlq), dtype=pl.Utf8),
            "PRIOR_DLQ_MONTHS": _nullable_int(np.where(is_null, 0, dlq), is_null),
            "MAX_DLQ_MONTHS_TO_DATE": pl.Series(self.max_dlq, dtype=pl.Int32),
            "N_DLQ_MONTHS_TO_DATE": pl.Series(self.n_dlq, dtype=pl.Int32),
            "PRIOR_DLQ_RUN_LENGTH": pl.Series(self.run_length, dtype=pl.Int32),
            "MONTHS_SINCE_FIRST_DLQ": _nullable_int(since, ~known),
            "EVER_DLQ_30_TO_DATE": pl.Series(self.max_dlq >= 1, dtype=pl.Boolean),
            "EVER_DLQ_90_TO_DATE": pl.Series(self.max_dlq >= 3, dtype=pl.Boolean),
            "PRIOR_IS_MODIFIED": pl.Series(self.is_modified, dtype=pl.Boolean),
        }

    # --- advancing --------------------------------------------------------
    def advance(
        self,
        dlq_now: np.ndarray,
        month_now: np.ndarray,
        update: np.ndarray | None = None,
        modified_now: np.ndarray | None = None,
    ) -> None:
        """Fold one realised month into the history, in place.

        ``dlq_now`` is the delinquency count the path actually landed on this
        month; ``update`` restricts the write to paths still on the book, so an
        absorbed path's history freezes at the month it left.

        ``modified_now`` exists for replaying observed history. Simulation leaves
        it None, which freezes the flag: a modification is a servicer decision
        driven by loss-mitigation policy, and nothing in this model predicts one.
        Freezing is the honest choice -- inventing modifications would fabricate
        the single most effective cure mechanism in the crisis vintages.
        """
        if update is None:
            update = np.ones(len(self), dtype=bool)

        # "RA" does not parse, so an REO month is NOT a delinquent month. This is
        # what the ETL's `.fill_null(False)` produces, and it is load-bearing:
        # the run counter resets on it.
        delinquent = dlq_now >= 1
        contributes = np.where(dlq_now < 0, 0, dlq_now)

        new_run = np.where(delinquent, self.run_length + 1, 0)
        new_max = np.maximum(self.max_dlq, contributes)
        new_n = self.n_dlq + delinquent.astype(np.int32)
        new_first = np.where(
            delinquent & (self.first_dlq_month < 0), month_now, self.first_dlq_month
        )

        self.dlq_months = np.where(update, dlq_now, self.dlq_months).astype(np.int32)
        self.run_length = np.where(update, new_run, self.run_length).astype(np.int32)
        self.max_dlq = np.where(update, new_max, self.max_dlq).astype(np.int32)
        self.n_dlq = np.where(update, new_n, self.n_dlq).astype(np.int32)
        self.first_dlq_month = np.where(update, new_first, self.first_dlq_month).astype(
            np.int32
        )
        if modified_now is not None:
            self.is_modified = np.where(update, modified_now, self.is_modified)


def replay(
    dlq_sequence: Sequence[int],
    periods: Sequence[str],
    modified: Sequence[bool] | None = None,
) -> pl.DataFrame:
    """Rebuild the path features for ONE observed delinquency sequence.

    The instrument behind the parity test: feed it a real loan's actual
    ``DLQ_MONTHS`` history and it must reproduce the ETL's columns exactly, row
    for row. Simulation replaces the observed sequence with a sampled one;
    everything else about the recursion is identical.
    """
    months = period_index(periods)
    history = PathHistory.fresh(1)
    rows = []
    for i, dlq in enumerate(dlq_sequence):
        month = months[i : i + 1]
        rows.append({k: v[0] for k, v in history.features(month).items()})
        history.advance(
            np.array([dlq], dtype=np.int32),
            month,
            modified_now=None if modified is None else np.array([modified[i]], dtype=bool),
        )
    return pl.DataFrame(rows)
