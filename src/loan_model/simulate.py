"""Monte Carlo path simulation.

The matrix projection in `project.py` advances a distribution over states. That
is exact for a first-order chain and structurally unable to represent duration
dependence: it holds one DLQ_30 state and applies the same average cure rate to
every loan in it, when the real rate runs from 51.4% (one month down) to 13.3%
(seven or more). Mass drains out of delinquency far too fast, never accumulates
in the deep states where charge-offs originate, and the 2007 backtest projects
1.57% cumulative credit events against an actual 8.97%.

Simulation replaces the distribution with sampled paths. Each path is a single
history, so the path-dependent features are exactly computable at every step
(`paths.PathHistory`) rather than excluded, and the true delinquency month count
is always available instead of pinned at the collapse threshold.

WHY THE LOOPS ARE THIS WAY ROUND
--------------------------------
The obvious implementation runs one path at a time. Measured on the real
`00` booster, a single-row predict costs ~3 ms of fixed overhead while 200,000
rows cost 334 ms -- 1.67 us/row. Per-path simulation of a 50,000-loan cohort over
120 months would issue 30M single-row calls and take roughly a day.

So the loops invert: **month outer, path inner**. At each month every active path
is grouped by from-state and scored in ONE batched call per from-state. The
predict-call count becomes horizon x |from_states|, INDEPENDENT of the number of
paths, and the same run finishes in seconds. Nothing in this module may contain a
per-path Python loop; `tests/test_simulate.py` asserts the call count directly.

Two consequences fall out of the same structure: absorbed paths leave the active
set and are never scored again, and the ~40 static features are written once
while only the ~25 dynamic ones are rewritten each month.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterator, Protocol, Sequence

import numpy as np
import polars as pl

from loan_etl.derive.amortization import scheduled_balance, scheduled_principal

from .paths import DlqCoding, PathHistory, period_index
from .registry import TransitionModel
from .scenario import MacroPanel
from .states import TransitionConfig
from .transition import StateSpace, predict_destinations

DEFAULT_LOSS_STATES = ("CHARGEOFF", "REO_DISPOSITION", "CREDIT_EVENT_OTHER")


class SimulationError(RuntimeError):
    pass


class SeverityModel(Protocol):
    """Loss given default. See `lgd.EmpiricalSeverity`."""

    def draw(self, frame: pl.DataFrame, uniforms: np.ndarray) -> np.ndarray: ...


@dataclass(frozen=True)
class FlatSeverity:
    """A single severity for every loan -- the matrix path's assumption."""

    value: float = 0.5

    def draw(self, frame: pl.DataFrame, uniforms: np.ndarray) -> np.ndarray:
        return np.full(frame.height, self.value, dtype=float)


# --- deterministic uniforms -------------------------------------------------
#
# Drawn from a hash of (seed, path, month) rather than a sequential generator.
# A per-chunk Generator would make results depend on --chunk-paths, so changing
# a purely operational knob would silently change the answer. Hashing also
# matches how `sampling.py` already makes its draws reproducible and streamable.

_GOLDEN = np.uint64(0x9E3779B97F4A7C15)
_MIX_A = np.uint64(0xBF58476D1CE4E5B9)
_MIX_B = np.uint64(0x94D049BB133111EB)


def _splitmix64(x: np.ndarray) -> np.ndarray:
    with np.errstate(over="ignore"):
        z = x.astype(np.uint64) + _GOLDEN
        z = (z ^ (z >> np.uint64(30))) * _MIX_A
        z = (z ^ (z >> np.uint64(27))) * _MIX_B
        return z ^ (z >> np.uint64(31))


def uniforms(path_keys: np.ndarray, month: int, seed: int, stream: int = 0) -> np.ndarray:
    """Reproducible U[0,1) per path for a given month and stream."""
    with np.errstate(over="ignore"):
        mixed = _splitmix64(
            path_keys.astype(np.uint64)
            ^ _splitmix64(np.uint64(month) * _GOLDEN + np.uint64(seed))
            ^ _splitmix64(np.full(len(path_keys), np.uint64(stream + 1), dtype=np.uint64))
        )
    # 53 bits is the most a float64 can hold exactly.
    return (mixed >> np.uint64(11)).astype(np.float64) * (2.0**-53)


def sample_destinations(proba: np.ndarray, draws: np.ndarray) -> np.ndarray:
    """Vectorised inverse-CDF sampling over each row's destination set."""
    cdf = np.cumsum(proba, axis=1)
    # Pin the final edge. Rows sum to 1 only up to float error, and a draw above
    # the last cumulative value would index off the end of the destination list.
    cdf[:, -1] = 1.0
    return (draws[:, None] > cdf).sum(axis=1).astype(np.int64)


# --- result -----------------------------------------------------------------


@dataclass
class SimulationResult:
    """Monthly stocks and flows, summed over every simulated path."""

    monthly: pl.DataFrame        # PERIOD x STATE: N, UPB, N_ENTERED, UPB_ENTERED, LOSS
    paths: pl.DataFrame | None
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def n_paths(self) -> int:
        return int(self.meta.get("paths", 0))

    def by_period(self) -> pl.DataFrame:
        """Portfolio totals per month, with the headline credit metrics."""
        loss_states = list(self.meta.get("loss_states", DEFAULT_LOSS_STATES))
        dlq_states = [s for s in self.meta.get("delinquent_states", ()) if s]

        return (
            self.monthly.group_by("PERIOD")
            .agg(
                pl.col("N").sum().alias("N_ACTIVE"),
                pl.col("UPB").sum().alias("UPB_ACTIVE"),
                pl.col("N").filter(pl.col("STATE").is_in(dlq_states)).sum().alias("N_DELINQUENT"),
                pl.col("UPB").filter(pl.col("STATE").is_in(dlq_states)).sum().alias("UPB_DELINQUENT"),
                pl.col("N_ENTERED").filter(pl.col("STATE").is_in(loss_states)).sum().alias("N_CREDIT_EVENT"),
                pl.col("N_ENTERED").filter(pl.col("STATE") == "PREPAID").sum().alias("N_PREPAID"),
                pl.col("UPB_ENTERED").filter(pl.col("STATE") == "PREPAID").sum().alias("UPB_PREPAID"),
                pl.col("LOSS").sum().alias("LOSS"),
                pl.col("CURTAILMENT").sum().alias("CURTAILMENT"),
            )
            .sort("PERIOD")
            .with_columns(
                (pl.col("N_DELINQUENT") / pl.col("N_ACTIVE")).alias("DELINQUENCY_RATE"),
                (pl.col("UPB_DELINQUENT") / pl.col("UPB_ACTIVE")).alias("DELINQUENCY_RATE_UPB"),
                pl.col("N_CREDIT_EVENT").cum_sum().alias("CUM_CREDIT_EVENTS"),
                pl.col("LOSS").cum_sum().alias("CUM_LOSS"),
            )
            .with_columns(
                # Single-monthly mortality: voluntary payoff plus curtailment as a
                # share of the balance that entered the month. Curtailment is a
                # third of all principal reduction, so a prepayment speed that
                # ignores it is not a prepayment speed.
                (
                    (pl.col("UPB_PREPAID") + pl.col("CURTAILMENT"))
                    / (pl.col("UPB_ACTIVE") + pl.col("UPB_PREPAID"))
                ).alias("SMM")
            )
            .with_columns(
                (1.0 - (1.0 - pl.col("SMM")) ** 12).alias("CPR")
            )
        )


# --- covariate advance ------------------------------------------------------


def _advance_period(period: str, months: int = 1) -> str:
    y, m = int(period[:4]), int(period[4:6])
    total = y * 12 + (m - 1) + months
    return f"{total // 12}{total % 12 + 1:02d}"


def _scheduled_upb(frame: pl.DataFrame, age: np.ndarray) -> np.ndarray:
    """Amortised balance for every path at its own age, in one expression."""
    return (
        frame.with_columns(pl.Series("_AGE", age, dtype=pl.Float64))
        .select(
            pl.when(pl.col("ORIGINAL_LOAN_TERM") > 0)
            .then(
                scheduled_balance(
                    pl.col("ORIGINAL_UPB").cast(pl.Float64),
                    pl.col("ORIGINAL_INTEREST_RATE").cast(pl.Float64),
                    pl.col("ORIGINAL_LOAN_TERM").cast(pl.Float64),
                    pl.col("_AGE"),
                )
            )
            .otherwise(pl.col("ORIGINAL_UPB").cast(pl.Float64))
            .fill_null(0.0)
            .alias("b")
        )["b"]
        .to_numpy()
    )


def principal_factor(dlq_months: np.ndarray, table: dict[str, float]) -> np.ndarray:
    """Share of the scheduled principal a loan in this state actually pays.

    A delinquent loan is not paying down. Observed medians: 0.202% of balance
    when current, 0.152% at one month, 0.088% at two, exactly 0.000% at 90+.
    Encoding that is what makes exposure at default come out right -- defaulted
    loans really do carry balances a median 2.4% ABOVE the origination schedule,
    which a closed-form scheduled balance can never produce.
    """
    out = np.full(len(dlq_months), float(table.get("3_plus", 0.0)))
    for months in (0, 1, 2):
        out[dlq_months == months] = float(table.get(str(months), 1.0 if months == 0 else 0.0))
    out[dlq_months < 0] = 0.0  # REO: no payment at all
    return out


def _scheduled_principal(
    frame: pl.DataFrame, balance: np.ndarray, remaining: np.ndarray
) -> np.ndarray:
    """Principal due this month on each path's OWN balance."""
    return (
        frame.with_columns(
            pl.Series("_BAL", balance, dtype=pl.Float64),
            pl.Series("_REM", remaining, dtype=pl.Float64),
        )
        .select(
            scheduled_principal(
                pl.col("_BAL"),
                pl.col("CURRENT_INTEREST_RATE").cast(pl.Float64).fill_null(0.0),
                pl.col("_REM"),
            )
            .fill_null(0.0)
            .clip(lower_bound=0.0)
            .alias("p")
        )["p"]
        .to_numpy()
    )


def _advance_covariates(
    frame: pl.DataFrame,
    *,
    period: str,
    age: np.ndarray,
    upb: np.ndarray,
    prior_upb: np.ndarray,
    scheduled_prin: np.ndarray,
    history: PathHistory,
    month: np.ndarray,
    macro: MacroPanel | None,
    features: set[str],
) -> pl.DataFrame:
    """Rewrite every time-varying feature for this month, all paths at once."""
    term = frame["ORIGINAL_LOAN_TERM"].cast(pl.Float64).fill_null(0.0).to_numpy()
    original = frame["ORIGINAL_UPB"].cast(pl.Float64).to_numpy()
    remaining = np.maximum(term - age, 0.0)

    updates: dict[str, Any] = {
        "LOAN_AGE": pl.Series(age, dtype=pl.Int32),
        "REMAINING_MONTHS_TO_LEGAL_MATURITY": pl.Series(remaining, dtype=pl.Float64),
        "MONTHLY_REPORTING_PERIOD": pl.Series([period] * frame.height, dtype=pl.Utf8),
        "PRIOR_UPB": pl.Series(prior_upb, dtype=pl.Float64),
        "SCHEDULED_UPB_AT_ORIGINATION_TERMS": pl.Series(upb, dtype=pl.Float64),
        "PRIOR_POOL_FACTOR": pl.Series(
            np.where(original > 0, prior_upb / np.where(original > 0, original, 1.0), np.nan),
            dtype=pl.Float64,
        ),
    }

    rate = frame["CURRENT_INTEREST_RATE"].cast(pl.Float64).fill_null(0.0).to_numpy()
    interest = prior_upb * rate / 1200.0
    # The scheduled principal of the loan's OWN balance, not the difference of two
    # closed-form scheduled balances. Those agree only while the loan tracks the
    # origination schedule, which is exactly what curtailment breaks.
    principal = scheduled_prin
    payment = np.where(remaining <= 0, prior_upb, interest + principal)
    updates["SCHEDULED_INTEREST"] = pl.Series(interest, dtype=pl.Float64)
    updates["SCHEDULED_PRINCIPAL"] = pl.Series(principal, dtype=pl.Float64)
    updates["SCHEDULED_PAYMENT"] = pl.Series(payment, dtype=pl.Float64)

    updates.update(history.features(month))

    # PRIOR_POOL_FACTOR and MONTHLY_REPORTING_PERIOD are written whether or not
    # they are model features: the mark-to-market LTV below is derived from the
    # pool factor, so omitting it when only MARK_TO_MARKET_LTV is a feature would
    # fail on a missing column.
    always = {"MONTHLY_REPORTING_PERIOD", "PRIOR_POOL_FACTOR"}
    frame = frame.with_columns(
        [s.alias(k) for k, s in updates.items() if k in features or k in always]
    )
    if macro is not None:
        frame = macro.attach(frame, period, columns=features)
    return _derive_macro_features(frame, features)


def _derive_macro_features(frame: pl.DataFrame, features: set[str]) -> pl.DataFrame:
    """Re-mark equity and refi incentive to the scenario's macro path.

    Without this the loan keeps its origination LTV for the whole horizon, so
    there is no negative-equity channel at all -- and negative equity is what
    actually drove 2007-2010 defaults (4.4x the charge-off rate, 2.3x lower cure).
    """
    cols = set(frame.columns)
    exprs = []

    if {"HPI_STATE", "HPI_AT_ORIGINATION"} <= cols:
        hpi_now, hpi_orig = pl.col("HPI_STATE"), pl.col("HPI_AT_ORIGINATION")
        usable = (
            hpi_now.is_not_null() & hpi_orig.is_not_null() & (hpi_orig > 0) & (hpi_now > 0)
        )
        if "HPI_GROWTH_SINCE_ORIGINATION" in features:
            exprs.append(
                pl.when(usable).then(hpi_now / hpi_orig - 1.0)
                .otherwise(None).alias("HPI_GROWTH_SINCE_ORIGINATION")
            )
        if "MARK_TO_MARKET_LTV" in features and "ORIGINAL_LOAN_TO_VALUE" in cols:
            exprs.append(
                pl.when(usable & pl.col("PRIOR_POOL_FACTOR").is_not_null())
                .then(
                    pl.col("ORIGINAL_LOAN_TO_VALUE")
                    * pl.col("PRIOR_POOL_FACTOR")
                    * hpi_orig
                    / hpi_now
                )
                .otherwise(None)
                .alias("MARK_TO_MARKET_LTV")
            )
    if exprs:
        frame = frame.with_columns(exprs)

    tail = []
    if "IS_NEGATIVE_EQUITY" in features and "MARK_TO_MARKET_LTV" in frame.columns:
        tail.append((pl.col("MARK_TO_MARKET_LTV") > 100.0).alias("IS_NEGATIVE_EQUITY"))
    # The matrix path leaves this frozen at origination; under a moving rate
    # scenario the refi incentive is exactly what drives prepayment.
    if "INTEREST_RATE_DIFF" in features and {"CURRENT_INTEREST_RATE", "MORTGAGE_RATE_30Y"} <= set(frame.columns):
        tail.append(
            (pl.col("CURRENT_INTEREST_RATE") - pl.col("MORTGAGE_RATE_30Y")).alias(
                "INTEREST_RATE_DIFF"
            )
        )
    if "UNEMPLOYMENT" in features and "UNEMPLOYMENT_STATE" in frame.columns:
        tail.append(pl.col("UNEMPLOYMENT_STATE").alias("UNEMPLOYMENT"))
    return frame.with_columns(tail) if tail else frame


# --- the engine -------------------------------------------------------------


def _path_keys(loans: pl.DataFrame, replicate: int) -> np.ndarray:
    """Stable 64-bit key per (loan, replicate), independent of chunking."""
    key = pl.concat_str(
        [pl.col("LOAN_SEQUENCE_NUMBER").cast(pl.Utf8), pl.lit(f"|{replicate}")]
    )
    return loans.select(key.hash(seed=0).alias("k"))["k"].to_numpy().astype(np.uint64)


def simulate_chunk(
    models: dict[str, TransitionModel],
    cfg: TransitionConfig,
    loans: pl.DataFrame,
    macro: MacroPanel | None,
    *,
    horizon: int,
    seed: int,
    replicate: int = 0,
    severity: SeverityModel | float = 0.5,
    start_period: str | None = None,
    start_state: str = "CURRENT",
    loss_states: Sequence[str] = DEFAULT_LOSS_STATES,
    collect_paths: bool = False,
    prepay_hazard: Any | None = None,
    curtailment: Any | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame | None]:
    """Simulate one chunk of paths forward ``horizon`` months.

    ``loans`` carries one row per path: every model feature plus the identifiers.
    Returns the monthly stock/flow aggregate and, optionally, the full paths.
    """
    if isinstance(severity, (int, float)):
        severity = FlatSeverity(float(severity))

    space = StateSpace.from_config(cfg)
    coding = DlqCoding.from_config(space, cfg)
    features = {f for m in models.values() for f in m.features}
    for extra in (prepay_hazard, curtailment):
        if extra is not None:
            features |= set(extra.features)
    factors = cfg.principal_paid_factor or {"0": 1.0, "1": 1.0, "2": 1.0, "3_plus": 0.0}
    n = loans.height

    # Route each state index to its model once, rather than per month.
    model_of = np.array(
        [cfg.model_key_for_state(s) if s in space.transient else "" for s in space.states],
        dtype=object,
    )
    absorbing = np.array([s in space.absorbing for s in space.states])
    loss_idx = {space.index(s) for s in loss_states if s in space.states}

    frame = loans
    keys = _path_keys(loans, replicate)
    state = np.full(n, space.index(start_state), dtype=np.int64)

    if "MONTHLY_REPORTING_PERIOD" in loans.columns:
        period = start_period or str(loans["MONTHLY_REPORTING_PERIOD"][0])
    else:
        period = start_period or "200701"

    history = (
        PathHistory.from_panel(loans)
        if "PRIOR_DLQ_MONTHS" in loans.columns
        else PathHistory.fresh(n)
    )
    age = (
        loans["LOAN_AGE"].fill_null(0).cast(pl.Int32).to_numpy().astype(np.int64)
        if "LOAN_AGE" in loans.columns
        else np.zeros(n, dtype=np.int64)
    )
    month0 = int(period_index([period])[0])
    term = loans["ORIGINAL_LOAN_TERM"].cast(pl.Float64).fill_null(0.0).to_numpy()

    # The loan's OWN balance, carried forward. Distinct from the closed-form
    # scheduled balance, which stays a feature under its own name: once a path
    # curtails or stops paying while delinquent it leaves the origination
    # schedule for good, and the schedule cannot represent that.
    balance = _scheduled_upb(frame, age)
    if "PRIOR_UPB" in loans.columns:
        # Seed from the real balance where there is one. Falls back PER ROW, not
        # per frame: a cohort observed from origination has no prior balance at
        # all, and filling those nulls with zero would start the whole book at a
        # zero balance and silently produce no losses.
        seeded = loans["PRIOR_UPB"].cast(pl.Float64).to_numpy()
        usable = np.isfinite(seeded) & (seeded > 0)
        balance = np.where(usable, seeded, balance)

    records: list[dict[str, Any]] = []
    path_rows: list[pl.DataFrame] = []

    for step in range(1, horizon + 1):
        prior_upb, prior_state = balance.copy(), state.copy()
        was_active = ~absorbing[state]
        if not was_active.any():
            break

        period = _advance_period(period)
        age = age + 1
        month = np.full(n, month0 + step, dtype=np.int32)
        upb = _scheduled_upb(frame, age)

        # Amortise the carried balance, scaled by how much of the scheduled
        # principal a loan in this delinquency state actually pays.
        remaining = np.maximum(term - age + 1, 0.0)
        due = _scheduled_principal(frame, prior_upb, remaining)
        paid = due * principal_factor(history.dlq_months, factors)
        balance = np.maximum(prior_upb - paid, 0.0)

        frame = _advance_covariates(
            frame, period=period, age=age, upb=upb, prior_upb=prior_upb,
            scheduled_prin=due, history=history, month=month, macro=macro,
            features=features,
        )

        # ONE batched predict per from-state. This is the whole design.
        #
        # Routing reads `prior_state` and writes a separate array. Scoring in
        # place would let a path move CURRENT -> DLQ_30 under the `00` model and
        # then be picked up again by `01` in the SAME month, cascading
        # CURRENT -> DLQ_30 -> DLQ_60 -> DLQ_90_PLUS -> CHARGEOFF in one step --
        # which is how a 2007 vintage produced charge-offs two months after
        # origination, an event that is impossible under any single model's
        # destination set.
        state = prior_state.copy()
        for from_state, model in models.items():
            rows = np.flatnonzero(was_active & (model_of[prior_state] == from_state))
            if rows.size == 0:
                continue
            proba = predict_destinations(model, frame[rows])
            picked = sample_destinations(proba, uniforms(keys[rows], step, seed))
            destinations = np.array(
                [space.index(d) for d in model.destinations], dtype=np.int64
            )
            state[rows] = destinations[picked]

        dlq_now = coding.next_months(state, history.dlq_months)
        history.advance(dlq_now, month, update=was_active)

        # Curtailment, applied only to paths still on the book after the
        # transition -- a loan that paid off in full this month has no partial
        # prepayment to make, and charging it one would double-count principal.
        curtailed = np.zeros(n, dtype=float)
        surviving = np.flatnonzero(was_active & ~absorbing[state])
        if prepay_hazard is not None and curtailment is not None and surviving.size:
            slice_ = frame[surviving]
            occurs = prepay_hazard.occurs(
                slice_, uniforms(keys[surviving], step, seed, stream=2)
            )
            fires = surviving[occurs]
            if fires.size:
                fraction = curtailment.sample(
                    frame[fires], uniforms(keys[fires], step, seed, stream=3)
                )
                curtailed[fires] = np.clip(fraction, 0.0, 1.0) * balance[fires]
                balance = balance - curtailed

        entered = was_active & absorbing[state]
        loss = np.zeros(n, dtype=float)
        hit = np.flatnonzero(entered & np.isin(state, list(loss_idx)))
        if hit.size:
            # The severity model conditions on WHICH outcome occurred -- realised
            # severity runs 53.8% for REO disposition against 37.0% for
            # charge-off -- so the destination travels with the slice under the
            # same column name the model was trained on.
            outcome = pl.Series("EVENT", [space.states[i] for i in state[hit]], dtype=pl.Utf8)
            lgd = severity.draw(
                frame[hit].with_columns(outcome),
                uniforms(keys[hit], step, seed, stream=1),
            )
            loss[hit] = prior_upb[hit] * lgd

        records.extend(
            _monthly_records(
                period, space, state, balance, prior_upb, absorbing, entered, loss, curtailed
            )
        )
        if collect_paths:
            path_rows.append(
                pl.DataFrame(
                    {
                        "LOAN_SEQUENCE_NUMBER": loans["LOAN_SEQUENCE_NUMBER"],
                        "REPLICATE": replicate,
                        "PERIOD": period,
                        "STATE": [space.states[i] for i in state],
                        "UPB": np.where(absorbing[state], 0.0, balance),
                        "SCHEDULED_UPB": upb,
                        "CURTAILMENT": curtailed,
                        "LOSS": loss,
                    }
                ).filter(pl.Series(was_active))
            )

    monthly = pl.DataFrame(records) if records else pl.DataFrame()
    paths = pl.concat(path_rows) if path_rows else None
    return monthly, paths


def _monthly_records(
    period: str,
    space: StateSpace,
    state: np.ndarray,
    balance: np.ndarray,
    prior_upb: np.ndarray,
    absorbing: np.ndarray,
    entered: np.ndarray,
    loss: np.ndarray,
    curtailed: np.ndarray,
) -> list[dict[str, Any]]:
    """Stocks and flows for one month, by bincount rather than group_by."""
    k = space.size
    live = ~absorbing[state]
    n_state = np.bincount(state[live], minlength=k)
    upb_state = np.bincount(state[live], weights=balance[live], minlength=k)
    curtail_state = np.bincount(state[live], weights=curtailed[live], minlength=k)

    idx = np.flatnonzero(entered)
    n_entered = np.bincount(state[idx], minlength=k)
    upb_entered = np.bincount(state[idx], weights=prior_upb[idx], minlength=k)
    loss_state = np.bincount(state[idx], weights=loss[idx], minlength=k)

    return [
        {
            "PERIOD": period,
            "STATE": s,
            "N": int(n_state[j]),
            "UPB": float(upb_state[j]),
            "N_ENTERED": int(n_entered[j]),
            "UPB_ENTERED": float(upb_entered[j]),
            "LOSS": float(loss_state[j]),
            "CURTAILMENT": float(curtail_state[j]),
        }
        for j, s in enumerate(space.states)
        if n_state[j] or n_entered[j]
    ]


def _chunks(loans: pl.DataFrame, size: int) -> Iterator[pl.DataFrame]:
    for start in range(0, loans.height, size):
        yield loans.slice(start, size)


def simulate(
    models: dict[str, TransitionModel],
    cfg: TransitionConfig,
    loans: pl.DataFrame,
    macro: MacroPanel | None = None,
    *,
    horizon: int = 120,
    seed: int | None = None,
    replicates: int | None = None,
    chunk_paths: int | None = None,
    severity: SeverityModel | float = 0.5,
    start_period: str | None = None,
    start_state: str = "CURRENT",
    loss_states: Sequence[str] = DEFAULT_LOSS_STATES,
    collect_paths: bool = False,
    progress: bool = False,
    prepay_hazard: Any | None = None,
    curtailment: Any | None = None,
) -> SimulationResult:
    """Simulate a portfolio, chunked so peak memory stays bounded.

    Every loan is simulated ``replicates`` times. One replicate is unbiased for
    portfolio aggregates once the book is large; more replicates tighten the tail
    percentiles, which is what a loss distribution actually needs.
    """
    sim = cfg.simulation if hasattr(cfg, "simulation") else {}
    seed = int(sim.get("seed", 20260814)) if seed is None else int(seed)
    replicates = int(sim.get("replicates", 1)) if replicates is None else int(replicates)
    chunk_paths = (
        int(sim.get("chunk_paths", 250_000)) if chunk_paths is None else int(chunk_paths)
    )
    if loans.height == 0:
        raise SimulationError("no loans to simulate")

    monthlies, path_frames = [], []
    total = 0
    for replicate in range(replicates):
        for chunk in _chunks(loans, chunk_paths):
            monthly, paths = simulate_chunk(
                models, cfg, chunk, macro,
                horizon=horizon, seed=seed, replicate=replicate, severity=severity,
                start_period=start_period, start_state=start_state,
                loss_states=loss_states, collect_paths=collect_paths,
                prepay_hazard=prepay_hazard, curtailment=curtailment,
            )
            if monthly.height:
                monthlies.append(monthly)
            if paths is not None:
                path_frames.append(paths)
            total += chunk.height
            if progress:
                print(f"    replicate {replicate} chunk of {chunk.height:,} done")

    combined = (
        pl.concat(monthlies)
        .group_by("PERIOD", "STATE")
        .agg(
            pl.col("N").sum(), pl.col("UPB").sum(),
            pl.col("N_ENTERED").sum(), pl.col("UPB_ENTERED").sum(),
            pl.col("LOSS").sum(), pl.col("CURTAILMENT").sum(),
        )
        .sort("PERIOD", "STATE")
        if monthlies
        else pl.DataFrame()
    )
    space = StateSpace.from_config(cfg)
    return SimulationResult(
        monthly=combined,
        paths=pl.concat(path_frames) if path_frames else None,
        meta={
            "paths": total,
            "loans": loans.height,
            "replicates": replicates,
            "horizon": horizon,
            "seed": seed,
            "start_state": start_state,
            "loss_states": list(loss_states),
            # Everything transient except CURRENT, read off the state space
            # rather than pattern-matched on names. Sorted so the metadata is
            # reproducible: `transient` is a frozenset and iterates arbitrarily.
            "delinquent_states": sorted(space.transient - {"CURRENT"}),
            "markov_safe_models": sorted(fs for fs, m in models.items() if m.markov_safe),
        },
    )
