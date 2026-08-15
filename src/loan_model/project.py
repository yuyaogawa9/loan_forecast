"""Multi-period forward projection.

Advances a distribution over states rather than simulating a path:

    pi_0 = one-hot on the loan's current state
    pi_t = pi_{t-1} . P_t
    EL   = sum_t sum_{a in default} (pi_t[a] - pi_{t-1}[a]) . UPB_t . severity

The matrix form sums over every path automatically, which a scalar
"multiply the survival probability forward" recursion cannot do once there is
more than one transient state.

`P_t` is rebuilt every month because it is not constant: loan age advances,
the balance amortises, and macro follows the supplied scenario. Exponentiating
a single matrix would assume a loan looks the same at month 1 and month 200.

Every model used here must be Markov-safe (see `columns.yaml`
`path_dependent_features`). A model fitted on "how many months has THIS loan
been delinquent" cannot be projected this way, because at step t the loan is
spread across states rather than on one path -- `assert_markov_safe` enforces it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Sequence

import numpy as np
import polars as pl

from loan_etl.derive.amortization import scheduled_balance

from .paths import status_strings
from .registry import TransitionModel
from .scenario import Scenario, scenario_from_macro_panel
from .states import TransitionConfig
from .transition import StateSpace, build_matrix

__all__ = [
    "DEFAULT_ABSORBING_LOSS_STATES",
    "ProjectionError",
    "ProjectionResult",
    "Scenario",
    "assert_markov_safe",
    "project_loan",
    "scenario_from_macro_panel",
]

DEFAULT_ABSORBING_LOSS_STATES = ("CHARGEOFF", "REO_DISPOSITION", "CREDIT_EVENT_OTHER")


class ProjectionError(RuntimeError):
    pass


def assert_markov_safe(models: dict[str, TransitionModel]) -> None:
    unsafe = sorted(fs for fs, m in models.items() if not m.markov_safe)
    if unsafe:
        raise ProjectionError(
            f"models {unsafe} were fitted with path-dependent features and cannot "
            "be projected by matrix recursion. Retrain with markov_safe=True, or "
            "forecast them by Monte Carlo path simulation instead."
        )


@dataclass
class ProjectionResult:
    periods: list[str]
    distribution: np.ndarray          # (horizon+1, K)
    balance: np.ndarray               # (horizon+1,) scheduled UPB
    space: StateSpace
    expected_loss: float
    loss_by_period: np.ndarray
    meta: dict[str, Any] = field(default_factory=dict)

    def to_frame(self) -> pl.DataFrame:
        data = {"PERIOD": self.periods, "SCHEDULED_UPB": self.balance}
        for j, s in enumerate(self.space.states):
            data[s] = self.distribution[:, j]
        data["INCREMENTAL_LOSS"] = self.loss_by_period
        return pl.DataFrame(data)

    def cumulative(self, state: str) -> np.ndarray:
        return self.distribution[:, self.space.index(state)]


def _advance_period(period: str, months: int = 1) -> str:
    y, m = int(period[:4]), int(period[4:6])
    total = y * 12 + (m - 1) + months
    return f"{total // 12}{total % 12 + 1:02d}"


def _scheduled_upb(row: dict[str, Any], age: int) -> float:
    frame = pl.DataFrame(
        {
            "p": [float(row.get("ORIGINAL_UPB") or 0.0)],
            "r": [float(row.get("ORIGINAL_INTEREST_RATE") or 0.0)],
            "n": [float(row.get("ORIGINAL_LOAN_TERM") or 0.0)],
            "t": [float(age)],
        }
    )
    if frame["n"][0] <= 0:
        return float(row.get("ORIGINAL_UPB") or 0.0)
    return float(
        frame.select(
            scheduled_balance(pl.col("p"), pl.col("r"), pl.col("n"), pl.col("t")).alias("b")
        )["b"][0]
    )


def _covariates_for_state(
    base: dict[str, Any],
    from_state: str,
    *,
    period: str,
    age: int,
    upb: float,
    prior_upb: float,
    macro: dict[str, Any],
    cfg: TransitionConfig,
    features: Sequence[str],
) -> pl.DataFrame:
    """One covariate row, conditional on the loan being in ``from_state``.

    PRIOR_DLQ_STATUS is the conditioning state expressed as Freddie's raw code,
    because in a Markov projection the conditioning state IS the previous status.

    It must be a code the fitted encoder actually knows. Passing the from-state
    key verbatim was wrong for the pooled deep-delinquency model, whose key is
    the bucket label "03_PLUS": encoders carry levels "03".."99", so
    `replace_strict` fell through to MISSING_CODE and deleted the strongest
    predictor in the one state where charge-offs originate.

    The month count still pins at the collapse threshold, since a distribution
    over states cannot say how long any particular loan has been down. That is
    inherent to matrix projection and is the reason `simulate.py` exists.
    """
    row = dict(base)
    row.update(macro)

    term = float(base.get("ORIGINAL_LOAN_TERM") or 0.0)
    row["LOAN_AGE"] = age
    row["REMAINING_MONTHS_TO_LEGAL_MATURITY"] = max(term - age, 0.0)
    row["PRIOR_UPB"] = prior_upb
    row["SCHEDULED_UPB_AT_ORIGINATION_TERMS"] = upb
    row["PRIOR_POOL_FACTOR"] = (
        prior_upb / base["ORIGINAL_UPB"] if base.get("ORIGINAL_UPB") else None
    )
    row["MONTHLY_REPORTING_PERIOD"] = period

    reo = cfg.from_state["reo_code"]
    months = None if from_state == reo else _months_of(from_state, cfg)
    row["PRIOR_DLQ_STATUS"] = reo if months is None else status_strings(np.array([months]))[0]
    row["PRIOR_DLQ_MONTHS"] = months

    # Re-mark the LTV to the scenario's house price path. Leaving it at its
    # origination value is exactly the mistake that made projected credit losses
    # ~6x too low: without a moving HPI there is no negative-equity channel, and
    # negative equity is what actually drove 2007-2010 defaults.
    hpi_now = row.get("HPI_STATE")
    hpi_orig = base.get("HPI_AT_ORIGINATION")
    pool = row.get("PRIOR_POOL_FACTOR")
    ltv0 = base.get("ORIGINAL_LOAN_TO_VALUE")
    if hpi_now and hpi_orig and hpi_orig > 0 and hpi_now > 0:
        row["HPI_GROWTH_SINCE_ORIGINATION"] = hpi_now / hpi_orig - 1.0
        if pool is not None and ltv0 is not None:
            mtm = ltv0 * pool * hpi_orig / hpi_now
            row["MARK_TO_MARKET_LTV"] = mtm
            row["IS_NEGATIVE_EQUITY"] = mtm > 100.0

    rate = float(base.get("CURRENT_INTEREST_RATE") or base.get("ORIGINAL_INTEREST_RATE") or 0.0)
    i = rate / 1200.0
    remaining = row["REMAINING_MONTHS_TO_LEGAL_MATURITY"]
    row["SCHEDULED_INTEREST"] = prior_upb * i
    row["SCHEDULED_PRINCIPAL"] = max(prior_upb - upb, 0.0)
    row["SCHEDULED_PAYMENT"] = row["SCHEDULED_INTEREST"] + row["SCHEDULED_PRINCIPAL"]
    if remaining <= 0:
        row["SCHEDULED_PAYMENT"] = prior_upb

    return pl.DataFrame({f: [row.get(f)] for f in features})


def _months_of(from_state: str, cfg: TransitionConfig) -> int | None:
    if from_state == cfg.from_state["deep_delinquency_label"]:
        return int(cfg.from_state["deep_delinquency_min_months"])
    try:
        return int(from_state)
    except ValueError:
        return None


def project_loan(
    models: dict[str, TransitionModel],
    cfg: TransitionConfig,
    loan: dict[str, Any],
    scenario: Scenario,
    *,
    horizon: int,
    start_state: str = "CURRENT",
    start_period: str | None = None,
    start_age: int = 0,
    severity: float = 0.5,
    loss_states: Sequence[str] = DEFAULT_ABSORBING_LOSS_STATES,
) -> ProjectionResult:
    """Project one loan (or a homogeneous cohort) forward `horizon` months."""
    assert_markov_safe(models)
    space = StateSpace.from_config(cfg)

    period = start_period or str(loan.get("MONTHLY_REPORTING_PERIOD") or "200701")
    pi = space.one_hot(start_state)

    periods = [period]
    dist = [pi.copy()]
    age = start_age
    upb = _scheduled_upb(loan, age)
    balances = [upb]
    losses = [0.0]
    loss_idx = [space.index(s) for s in loss_states if s in space.states]

    total_loss = 0.0
    for _ in range(horizon):
        prev_pi, prior_upb = pi, upb
        period = _advance_period(period)
        age += 1
        upb = _scheduled_upb(loan, age)
        macro = scenario.row_for(period)

        covariates = {
            fs: _covariates_for_state(
                loan, fs, period=period, age=age, upb=upb, prior_upb=prior_upb,
                macro=macro, cfg=cfg, features=models[fs].features,
            )
            for fs in models
        }
        P = build_matrix(models, covariates, space, cfg)
        pi = prev_pi @ P

        # Incremental absorption this month, valued at the balance that was at
        # risk entering it.
        incremental = float(sum(pi[i] - prev_pi[i] for i in loss_idx))
        step_loss = max(incremental, 0.0) * prior_upb * severity
        total_loss += step_loss

        periods.append(period)
        dist.append(pi.copy())
        balances.append(upb)
        losses.append(step_loss)

    return ProjectionResult(
        periods=periods,
        distribution=np.vstack(dist),
        balance=np.asarray(balances),
        space=space,
        expected_loss=total_loss,
        loss_by_period=np.asarray(losses),
        meta={
            "horizon": horizon,
            "start_state": start_state,
            "severity": severity,
            "loss_states": [space.states[i] for i in loss_idx],
        },
    )
