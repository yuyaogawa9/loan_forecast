"""Fit the conditional amount models from the curated panel.

Mirrors `train.py`, which does the same for the transition multinomials. Three
models, each answering "how much, given that this happened":

``LOSS_SEVERITY``          given a credit event, conditioned on WHICH one
``IS_PARTIAL_PREPAYMENT``  monthly hazard of a curtailment
``CURTAILMENT_FRACTION``   size of that curtailment, as a share of the balance

Full prepayment gets no model. ``ZERO_BALANCE_REMOVAL / PRIOR_UPB`` is exactly
1.0000 at the 10th, 50th and 90th percentiles -- a full payoff retires the
balance and nothing else -- so fitting it would be estimating a constant.
"""

from __future__ import annotations

from typing import Any, Sequence

import polars as pl

from loan_etl.features import select
from loan_etl.io import scan_dataset
from loan_etl.settings import Settings

from .amounts import (
    DEFAULT_QUANTILES,
    BinaryHazardModel,
    QuantileAmountModel,
    quantile_calibration,
    save_amount_model,
)
from .sampling import HASH_MODULUS
from .states import TransitionConfig, load_transition_config

SEVERITY_TARGET = "LOSS_SEVERITY"
HAZARD_TARGET = "IS_PARTIAL_PREPAYMENT"
CURTAILMENT_TARGET = "CURTAILMENT_FRACTION"

# Severity is loss / defaulted balance, and that ratio is meaningless once the
# denominator approaches zero. `severity.py` only guards ZERO_BALANCE_REMOVAL > 0,
# which lets through loans already paid down to nothing: nine observations carry
# a balance of $0.01 to $517 against a median of $141,286, and they produce
# severities up to 657,341. In dollars they are irrelevant -- 0.005% of all loss
# -- but a bucketed sampler drawing one of them would book a catastrophic phantom
# loss, so they are excluded at the point the ratio is consumed rather than
# rebuilt into the panel.
MIN_DEFAULTED_BALANCE = 1_000.0

# Below this the tail quantiles cannot be early-stopped reliably.
MIN_VALIDATION_ROWS = 2_000

# The outcome the amount is conditional on. Deliberately a FEATURE of the
# severity model, not a split: realised severity differs sharply by outcome
# (REO_DISPOSITION 53.8%, CHARGEOFF 37.0%, CREDIT_EVENT_OTHER 34.6% medians), but
# there are only ~4.3k charge-offs, so one model sharing strength across outcomes
# beats three thin ones.
OUTCOME_FEATURE = "EVENT"

# Never allowed near the severity model: each is a component of, or algebraically
# equal to, the quantity being predicted. Conditioning on the OUTCOME is the
# point; conditioning on the realised loss would be circular.
SEVERITY_FORBIDDEN = (
    "LOSS_SEVERITY", "LOSS_AMOUNT", "ACTUAL_LOSS_CALCULATION", "RECONSTRUCTED_LOSS",
    "TOTAL_RECOVERIES", "TOTAL_EXPENSES", "TOTAL_EXPENSES_SUM", "RECOVERY_RATE",
    "ZERO_BALANCE_REMOVAL", "NET_SALE_PROCEEDS_AMT", "MI_RECOVERIES",
    "NON_MI_RECOVERIES", "DELINQUENT_ACCRUED_INTEREST", "CURRENT_ACTUAL_UPB",
)


class AmountTrainingError(RuntimeError):
    pass


def _hash_filter(seed: int, rate: float) -> pl.Expr:
    """Deterministic uniform subsample, streaming-safe.

    Same idiom as `sampling._hash_expr`: reproducible without holding the frame
    in memory, which `.sample()` cannot do.
    """
    key = pl.concat_str(
        [pl.col("LOAN_SEQUENCE_NUMBER"), pl.col("MONTHLY_REPORTING_PERIOD")], separator="|"
    )
    return (key.hash(seed=seed) % HASH_MODULUS) < rate * HASH_MODULUS


def _safe_features(panel_columns: Sequence[str], target: str) -> list[str]:
    """Leakage-screened features for an amount target."""
    cols = select(list(panel_columns), target)
    return list(cols.features)


def _time_split(df: pl.DataFrame, through_year: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    year = pl.col("MONTHLY_REPORTING_PERIOD").str.slice(0, 4).cast(pl.Int32)
    return df.filter(year <= through_year), df.filter(year > through_year)


# --- training frames --------------------------------------------------------


def severity_frame(settings: Settings) -> tuple[pl.DataFrame, list[str]]:
    """Every credit event carrying a realised severity (~19.6k rows).

    Small enough to keep whole -- no sampling needed, and none wanted: these are
    the rarest and most valuable observations in the panel.
    """
    lf = scan_dataset(settings.curated / "loan_month")
    panel_columns = lf.collect_schema().names()
    features = [
        f
        for f in _safe_features(panel_columns, "loss_severity")
        if f not in SEVERITY_FORBIDDEN
    ]
    if OUTCOME_FEATURE in panel_columns:
        features.append(OUTCOME_FEATURE)

    keep = ["LOAN_SEQUENCE_NUMBER", "MONTHLY_REPORTING_PERIOD", SEVERITY_TARGET, *features]
    frame = (
        lf.filter(
            pl.col("IS_CREDIT_EVENT")
            & pl.col(SEVERITY_TARGET).is_not_null()
            & (pl.col("ZERO_BALANCE_REMOVAL") >= MIN_DEFAULTED_BALANCE)
        )
        .select([c for c in dict.fromkeys(keep) if c in panel_columns])
        .collect(engine="streaming")
    )
    return frame, [f for f in features if f in frame.columns]


def curtailment_frames(
    settings: Settings, *, seed: int, hazard_rate: float, amount_rate: float
) -> tuple[pl.DataFrame, pl.DataFrame, list[str]]:
    """Hazard and amount frames for partial prepayment.

    Both subsampled UNIFORMLY, not case-control: partial prepayment happens in
    ~21% of performing loan-months, so it is not a rare event and a uniform draw
    keeps the rate unbiased without needing weights.

    Restricted to CURTAILMENT_RELIABLE rows throughout. Where the disclosed
    balance is rounded to the nearest $1,000, curtailment below that is simply
    not observable, and training on quantisation noise would manufacture
    prepayments that never happened.
    """
    lf = scan_dataset(settings.curated / "loan_month")
    panel_columns = lf.collect_schema().names()
    features = _safe_features(panel_columns, "curtailment_amount")

    base = lf.filter(
        pl.col("IS_MODELABLE")
        & pl.col("CURTAILMENT_RELIABLE")
        & (pl.col("PRIOR_UPB") > 0)
        & ~pl.col("IS_TERMINAL")
    )
    keep = [
        "LOAN_SEQUENCE_NUMBER", "MONTHLY_REPORTING_PERIOD",
        HAZARD_TARGET, "CURTAILMENT", "PRIOR_UPB", *features,
    ]
    keep = [c for c in dict.fromkeys(keep) if c in panel_columns]

    hazard = (
        base.filter(_hash_filter(seed, hazard_rate))
        .select(keep)
        .collect(engine="streaming")
    )
    amount = (
        base.filter(pl.col(HAZARD_TARGET) & _hash_filter(seed + 1, amount_rate))
        .select(keep)
        .with_columns(
            (pl.col("CURTAILMENT") / pl.col("PRIOR_UPB")).alias(CURTAILMENT_TARGET)
        )
        .collect(engine="streaming")
    )
    return hazard, amount, [f for f in features if f in hazard.columns]


# --- orchestration ----------------------------------------------------------


def train_amount_models(
    settings: Settings,
    cfg: TransitionConfig | None = None,
    *,
    name: str,
    only: Sequence[str] | None = None,
) -> dict[str, Any]:
    cfg = cfg or load_transition_config()
    spec = dict(cfg.amounts or {})
    quantiles = tuple(spec.get("quantiles", DEFAULT_QUANTILES))
    seed = int(spec.get("seed", 20260814))
    through = int(cfg.training["train_through_year"])
    wanted = set(only) if only else None
    results: dict[str, Any] = {}

    def selected(target: str) -> bool:
        return wanted is None or target in wanted

    if selected(SEVERITY_TARGET):
        frame, features = severity_frame(settings)
        if frame.height == 0:
            raise AmountTrainingError("no credit events with a realised severity")
        train, valid = _time_split(frame, through)
        # Crisis-vintage dispositions are nearly all pre-2021, so a strict time
        # split leaves too little to early-stop on: with 700 validation rows the
        # extreme-quantile objective is so noisy that the 0.99 booster stopped
        # after ONE iteration and became a constant -- precisely the quantile
        # that sets the loss tail. Fall back to a deterministic random holdout
        # for fitting; out-of-time calibration is still reported separately.
        out_of_time = valid
        split = "out_of_time"
        if valid.height < MIN_VALIDATION_ROWS:
            train, valid = _holdout_split(frame, seed)
            split = "random_holdout"
        model = QuantileAmountModel.fit(
            train, SEVERITY_TARGET, features, valid=valid, quantiles=quantiles,
            categorical_hint=(OUTCOME_FEATURE,),
        )
        model.metrics["validation_split"] = split
        # The honest out-of-time check, reported even when a random holdout was
        # used for early stopping.
        if split != "out_of_time" and out_of_time.height:
            model.metrics["out_of_time_calibration"] = quantile_calibration(
                model, out_of_time
            ).to_dicts()
        save_amount_model(settings, name, model)
        results[SEVERITY_TARGET] = model.metrics

    if selected(HAZARD_TARGET) or selected(CURTAILMENT_TARGET):
        hazard_df, amount_df, features = curtailment_frames(
            settings,
            seed=seed,
            hazard_rate=float(spec.get("hazard_rate", 0.05)),
            amount_rate=float(spec.get("amount_rate", 0.10)),
        )
        if selected(HAZARD_TARGET):
            train, valid = _time_split(hazard_df, through)
            model = BinaryHazardModel.fit(train, HAZARD_TARGET, features, valid=valid)
            save_amount_model(settings, name, model)
            results[HAZARD_TARGET] = model.metrics
        if selected(CURTAILMENT_TARGET):
            train, valid = _time_split(amount_df, through)
            model = QuantileAmountModel.fit(
                train, CURTAILMENT_TARGET, features, valid=valid, quantiles=quantiles,
                clip=(0.0, 1.0),
            )
            save_amount_model(settings, name, model)
            results[CURTAILMENT_TARGET] = model.metrics

    return {"name": name, "targets": sorted(results), "metrics": results}


def _holdout_split(frame: pl.DataFrame, seed: int, rate: float = 0.25):
    mask = frame.select(_hash_filter(seed + 7, rate).alias("h"))["h"]
    return frame.filter(~mask), frame.filter(mask)
