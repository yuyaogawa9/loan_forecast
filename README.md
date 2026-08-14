# loan_forecast

Forecasting default, delinquency, charge-off, recovery, prepayment and loss
severity on the **Freddie Mac Single-Family Loan-Level Dataset**.

The dataset is far larger than this machine's memory, so the pipeline is built
around lazy, streaming evaluation with **polars**: source text is scanned
straight into partitioned parquet without ever being fully materialised, and
model estimation runs on bootstrapped samples.

---

## Quick start

```bash
pip install -e ".[model,dev]"
cp .env.example secret.env      # then fill in FRED_API_KEY and DATA_ROOT

# Place the Sample dataset under $DATA_ROOT/raw/freddie as either
#   sample_2007/sample_orig_2007.txt + sample_svcg_2007.txt
#   sample_2007.zip
python -m run.build_dataset --stage all --vintages all
```

Useful variants:

```bash
python -m run.build_dataset --stage ingest --vintages 1999-2010
python -m run.build_dataset --stage all --vintages 2007 --skip-macro   # no FRED key
python -m run.build_dataset --stage validate --vintages all
pytest                                                                  # 43 tests
```

## Pipeline stages

| Stage | Input | Output |
|---|---|---|
| `ingest` | `raw/freddie/sample_YYYY/*.txt` | `bronze/{origination,performance}/vintage_year=YYYY/` |
| `macro` | FRED / ALFRED API | `curated/macro_monthly.parquet` |
| `curate` | bronze + macro | `curated/loan_month/`, `curated/loan_outcomes/` |
| `validate` | everything | pass/fail gates + `_manifests/*.json` |

`loan_month` is the all-encompassing panel — every cleaned origination and
performance field, plus event flags, amortization and curtailment, loss and
severity, and point-in-time macro. One row per loan per reporting month.
`loan_outcomes` collapses that to one row per loan for survival models.

Every stage is idempotent: a vintage whose source checksum matches its manifest
is skipped unless `--force`. Raw text is extracted one vintage at a time and
deleted after conversion, which is what keeps peak disk within budget.

## Layout

```
config/schemas/     versioned field specs (YAML) -- the single source of truth
src/loan_etl/
  schema.py         YAML -> polars dtypes, cast + sentinel expressions
  settings.py       secret.env -> typed Settings
  io.py             hive partitioning, manifests, checksums
  acquire/          freddie.py (source registry), fred.py (ALFRED PIT)
  clean/ingest.py   scan_csv -> typed bronze parquet
  derive/           amortization, events, severity, macro_join, panel
  validate.py       gates
run/build_dataset.py  CLI orchestrator
functions/loan_xgb.py modelling helpers (unchanged)
```

## Modelling from the panel

`curated/loan_month` is a discrete-time hazard panel: one row per loan per
reporting month, 129 columns. Never train on it directly — it deliberately
retains post-outcome fields. Go through `features`, which enforces the split:

```python
import polars as pl
from loan_etl.settings import get_settings
from loan_etl.io import scan_dataset
from loan_etl.features import training_frame

s = get_settings()
panel = scan_dataset(s.curated / "loan_month")

lf, cols = training_frame(panel, "default")     # safe features only; 31 leakage columns blocked
train = lf.filter(pl.col("MONTHLY_REPORTING_PERIOD").str.slice(0, 4) <= "2021").collect()

import xgboost as xgb
model = xgb.XGBClassifier(enable_categorical=True, tree_method="hist")
model.fit(train.select(cols.features).to_pandas(), train[cols.target].to_pandas())
```

`training_frame` returns a frame XGBoost accepts directly: string features are
cast to `pl.Categorical` (derived from dtype, so none can be missed), and
degenerate features — entirely null or constant — are dropped and listed in
`cols.dropped_degenerate`. That drop is not cosmetic: an all-null categorical
has zero levels and XGBoost's categorical path *raises* on it, and fields added
in later releases (`PROPERTY_VALUATION_METHOD`, `SUPER_CONFORMING_FLAG`) are
null for every loan in early vintages.

**Prior delinquency will dominate any delinquency model**, because delinquency
persists — a loan 60 days down is overwhelmingly likely to be 90 days down next
month. That produces a high AUC carrying little credit-risk signal. Condition on
the starting state instead:

```python
from loan_etl.features import starting_state
new_dlq = starting_state(panel, "0")     # loans that were current last month
```

Available targets: `delinquency_30`, `delinquency_60`, `delinquency_90`,
`serious_delinquency`, `default`, `chargeoff`, `prepayment`,
`partial_prepayment`, `curtailment_amount`, `scheduled_payment`,
`recovery_rate`, `loss_severity`, and `competing_risks` (the mutually-exclusive
`EVENT` label: `CURRENT` / `DLQ_30` / `DLQ_60` / `DLQ_90_PLUS` / `REO` /
`PREPAID` / `MATURED` / `CHARGEOFF` / `REO_DISPOSITION` / `CREDIT_EVENT_OTHER` /
`CENSORED`).

Every column carries a role in `config/schemas/columns.yaml`:

| Role | Meaning |
|---|---|
| `feature_static` (32) | Origination attributes — fixed for the life of the loan |
| `feature_dynamic` (19) | Known at the **start** of month *t*: `PRIOR_*` lags, `MAX_DLQ_MONTHS_TO_DATE`, `PRIOR_DLQ_RUN_LENGTH`, scheduled P&I |
| `feature_macro` (9) | Published and available by month *t* |
| `target` (29) | Describes what happened **during** month *t* |
| `leakage` (31) | Contemporaneous with or downstream of the outcome |
| `identifier` / `metadata` (18) | Keys and build diagnostics |

The distinction is **timing, not topic**. `CURRENT_LOAN_DELINQUENCY_STATUS` at
*t* is the month-*t* outcome restated, so it is leakage; `PRIOR_DLQ_MONTHS` is
the same information lagged, and is a feature. Likewise `CURRENT_ACTUAL_UPB` is
leakage while `PRIOR_UPB` is not.

Three gates enforce this: `all_columns_classified` fails on any column the
registry doesn't describe (so the panel can't grow past the registry),
`features_exclude_leakage` re-checks every declared target, and
`lagged_state_is_causal` verifies each `PRIOR_*` value really equals the prior
month's within the same loan.

`training_frame` filters to `IS_MODELABLE` by default, dropping each loan's
first observation — it has no lagged state by construction.

## Design notes

**Schema as data.** Field positions, dtypes, sentinels and valid ranges live in
`config/schemas/*_v47.yaml`, not in code. Freddie's layout is versioned and does
change (Release 47, July 2026), so a layout change is a new YAML file rather
than an edit to a parser. A field-count mismatch **raises**; it never silently
drops rows.

**Sentinels are mapped before casting.** `CREDIT_SCORE=9999`, `DTI=999`,
`LTV/CLTV=999`, `MI%=999`, `UNITS=99`, `POSTAL=00000` all become null. A 9999
credit score reaching a model as a real number invalidates results, so
`no_sentinel_leakage` is a hard gate.

**Everything is read as text first.** `NET_SALE_PROCEEDS` mixes numeric amounts
with the codes `C` (proceeds covered the loss) and `U` (unknown). A reader-level
numeric cast destroys them. It is split into `_CODE` and `_AMT`, and loss
reconstruction is set to null — not zero — where the amount is unquantified.

**Delinquency status stays categorical.** `"RA"` (REO acquisition) and `"XX"`
(unknown) are not numbers. `DLQ_MONTHS` is exposed separately.

**Losses are reconstructed independently.** Freddie's `ACTUAL_LOSS_CALCULATION`
is ground truth for severity, but the loss is also rebuilt from UPB, accrued
interest, expenses and recoveries. The two must agree for ≥99% of disposed
loans, which is what catches a flipped sign convention.

**Partial prepayment needs a threshold, not a tolerance.** Curtailment is actual
principal reduction beyond the schedule, recomputed each month from the
*current* rate and remaining term so modifications don't register as
prepayments. It must clear both an absolute floor and 5 bp of balance; UPB
rounding granularity is measured per vintage and raises the floor when Freddie
has quantised balances.

**Macro data is point-in-time.** Two separate biases are handled: revision bias
(ALFRED first-release rather than latest vintage) and publication lag (a March
loan-month sees February's print). The NBER recession indicator is dated
retroactively and is excluded unless `--include-pit-unsafe`.

**Nothing is dropped silently.** Structurally invalid rows go to
`quarantine/`, and `row_accounting` asserts `written + quarantined == source`.

## Requirements

Python 3.10+, polars ≥ 1.25, pyarrow, duckdb, fredapi, python-dotenv, pyyaml.
A free [FRED API key](https://fred.stlouisfed.org/docs/api/api_key.html) for the
macro stage; `--skip-macro` builds without one.

## Data access

The Sample dataset (50,000 loans per full vintage year) and the full Standard
dataset are both downloaded manually from
[Clarity Data Intelligence](https://capitalmarkets.freddiemac.com/clarity),
which is a registered-session portal with no public bulk-download API. This
repo owns the *registry* of what you downloaded — checksums, line counts,
ingest state — not a scraper.

Sizing, if you move beyond the Sample dataset: the full Standard set is ~55M
loans (originations 1999-01-01 → 2025-09-30) and roughly 3–4 billion performance
rows, which is ~30–50 GB even as well-compressed parquet. Point `DATA_ROOT` at
external storage before attempting it.