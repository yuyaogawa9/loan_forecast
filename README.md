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
pytest                                                                  # 157 tests
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
config/
  schemas/            versioned source-file specs + the column role registry
  transitions.yaml    state space, destination collapse, sampling, training, simulation
src/loan_etl/         raw text -> curated loan-month panel
  schema.py           YAML -> polars dtypes, cast + sentinel expressions
  settings.py         secret.env -> typed Settings
  io.py               hive partitioning, manifests, checksums, data lock
  features.py         leakage-screened feature selection
  acquire/            freddie.py (source registry), fred.py (ALFRED PIT macro)
  clean/ingest.py     scan_csv -> typed bronze parquet
  derive/             amortization, events, severity, macro_join, panel
  validate.py         gates
src/loan_model/       panel -> transition models -> forecast
  states.py           state space + from-state bucketing
  sampling.py         deterministic case-control sampler
  dataset.py          polars -> Arrow -> LightGBM (no pandas)
  train.py            per-from-state multinomials
  evaluate.py         unsampled-holdout calibration
  transition.py       row-stochastic matrix assembly
  project.py          matrix projection under a macro scenario
  paths.py            per-path delinquency history (parity with the ETL)
  simulate.py         Monte Carlo path simulation, batched month-outer
  scenario.py         Scenario (one state) + MacroPanel (all 52, batched)
  lgd.py              bucketed empirical severity (baseline)
  amounts.py          quantile-regression amount models + binary hazard
  train_amounts.py    fits severity, prepay hazard and curtailment
  registry.py         model persistence
run/
  build_dataset.py    ETL CLI
  train_models.py     modelling CLI
  forecast.py         projection + simulation CLI
tests/                157 tests
```

Installed as a package, the three CLIs are also available as `build-dataset`,
`train-models` and `forecast`.

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

lf, cols = training_frame(panel, "default")   # safe features only; 31 leakage columns blocked
train = lf.filter(pl.col("MONTHLY_REPORTING_PERIOD").str.slice(0, 4) <= "2021").collect()
```

For the multi-state models the pipeline uses, go through `run/train_models.py`
rather than hand-rolling a fit -- see the next section.

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

## Multi-state transition models

Eight -- now five -- LightGBM multinomials, one per from-state, each predicting
only the destinations that state actually reaches. A softmax over a from-state's
destinations sums to 1 without renormalisation, so the transition matrix is
row-stochastic by construction.

```bash
python -m run.train_models --stage all          # sample -> train -> evaluate -> verify
python -m run.forecast --vintage 2007 --horizon 180 --severity 0.52
```

**Scale.** The panel is 73.6M modelable loan-months, of which `CURRENT -> CURRENT`
is 95.15%. Case-control sampling keeps 100% of every informative stratum and
subsamples that one at 5%, re-weighting by `1/rate`:

| | |
|---|---|
| 73,556,410 -> 7,049,517 rows | 10.4x reduction, 94s |
| all 77 strata reproduce population counts | worst 0.08% (hash noise) |
| sampler peak RSS | 2.35 GB |
| training peak RSS, all models | 2.30 GB, 102s |

Ingestion is polars -> integer codes -> Arrow -> LightGBM. **pandas is never
constructed**, which is what the old `to_pandas()` path made impossible: 73.6M x
60 float64 is ~35 GB. A test fails if `pandas.DataFrame` appears in that path.

**Two alphabets must match.** Projection occupies transient *states*
(`CURRENT, DLQ_30, DLQ_60, DLQ_90_PLUS, REO`) while models are keyed by
from-state, so the from-state buckets have to collapse at the same point EVENT
does. The first version bucketed at 6 months while EVENT collapses at 3, which
left `DLQ_90_PLUS` routed to a model with no CHARGEOFF or REO destination:
default mass cycled forever and projected losses came out as exactly zero, with
no error anywhere. `load_transition_config` now asserts the alphabets align, and
`build_matrix` refuses a state with no model rather than self-looping.

**Calibration** is checked on UNSAMPLED holdout data (2022-23), never on the
training sample -- otherwise it would measure the model against the distortion
it is meant to have corrected. Worst absolute error 6.5%, most 2-4%.
Discrimination is strong for prepayment (AUC 0.85-0.92 across every state) and
for deep delinquency (0.69-0.87 from 90+), weak for shallow roll (0.54-0.63),
and absent for REO disposition timing (0.49) and for censoring (0.49) -- both
administrative events that loan characteristics genuinely do not predict.

### House prices and the negative-equity channel

State HPI (FHFA via FRED, `{ST}STHPI`, 51 states -- FRED has no `PRSTHPI`) feeds
three derived features:

```
MARK_TO_MARKET_LTV = ORIGINAL_LTV * PRIOR_POOL_FACTOR * HPI_at_origination / HPI_now
HPI_GROWTH_SINCE_ORIGINATION
IS_NEGATIVE_EQUITY
```

All three are Markov-safe: origination HPI is a fixed loan attribute, current HPI
comes from the scenario, and the pool factor follows the amortisation schedule,
so the projection re-marks the LTV every step instead of freezing it.

The signal is real. On the 2007 vintage, negative equity peaks at **22.8% in
2012** against 1.1% at origination, tracking California's 39.1% peak-to-trough
decline. And from 90+ days delinquent, negative equity means:

| | negative equity | positive equity |
|---|---|---|
| charge-off | 1.50% | 0.34% |
| cure | 2.78% | 6.29% |

**4.4x the charge-off rate and 2.3x lower cure.**

### The gap was structural, not a missing feature

Adding HPI improved the 2007 backtest (credit events 1.22% -> 1.57% once
projected over state x LTV segments rather than one average loan), but actual is
8.97%. The cause is now diagnosed, and it is not the models:

**The one-month transition models are accurate.** On 839,961 real California
loan-months from CURRENT during 2009-2011 -- the worst of the crisis -- predicted
versus actual is CURRENT 0.9746/0.9757, DLQ_30 0.0072/0.0081, PREPAID
0.0167/0.0160. Ratios of 0.89-1.05.

**The projection is what breaks.** Delinquency has strong duration dependence.
From DLQ_30, the cure rate depends on how long the loan has already been down:

| spell length | cure rate |
|---|---|
| 1 month | 51.4% |
| 2-3 months | 30.9% |
| 4-6 months | 19.7% |
| 7+ months | 13.3% |

A 3.9x spread. A first-order Markov chain has one DLQ_30 state, so it applies the
same ~44% average cure to every loan in it -- a figure dominated by the
newly-delinquent majority. Mass therefore drains out of delinquency far too fast
and never accumulates in the deep states where charge-offs originate. The
Markov-safe feature restriction is *correct* for matrix projection; matrix
projection is what is inadequate.

There were two ways forward -- Monte Carlo path simulation, or expanding the
state space to encode duration (`DLQ_30_NEW` vs `DLQ_30_SEASONED`, keeping the
matrix form but multiplying states and models). The first is implemented, and it
closes about half the gap. See the backtest below.

## Monte Carlo simulation

```bash
python -m run.train_models --stage train --allow-path-dependent --name transitions_full
python -m run.forecast --vintage 2007 --method mc --name transitions_full \
    --horizon 120 --empirical-severity
```

Each path is a single history, so the seven path-dependent features are exactly
computable at every step rather than excluded, and the true delinquency month
count survives instead of pinning at the collapse threshold. Simulating the
*actual* loans also removes the convexity bias of projecting one average loan.

**Why it is affordable: the loops run month-outer, path-inner.** Measured on the
real `00` booster:

| batch | total | per row |
|---|---|---|
| 1 | 93.4 ms | 93,390 us |
| 1,000 | 5.3 ms | 5.31 us |
| 200,000 | 334.2 ms | 1.67 us |

Fixed per-call overhead dominates below ~1,000 rows. So every active path is
grouped by from-state and scored in ONE batched call per from-state per month:
predict calls come to `horizon x |from_states|`, **independent of path count**.
Per-path simulation of a 50,000-loan cohort would issue 30M single-row calls and
take about a day; batched, the same run is seconds. `test_simulate.py` asserts
the call-count bound directly, because a regression here would make simulation
unusable at portfolio size rather than visibly wrong.

**Correctness rests on `paths.py` reproducing the ETL exactly.** If the simulator
derives history features even slightly differently from `with_panel_state`, the
model receives a covariate distribution it never saw in training -- and the
resulting probabilities are wrong with no error anywhere. The gate is a parity
test that replays real loans' *observed* delinquency sequences through the
simulator's own recursion: **exact parity on all nine features across 386,436
modelable loan-months from 6,000 real loans.**

Two ETL behaviours are deliberately preserved rather than "fixed", because they
are what training saw: an REO month is not a delinquent month (`"RA"` does not
parse, so it breaks the delinquency run), and `DLQ_90_PLUS` is a lumped state
whose underlying month counter keeps climbing.

## Conditional amount models

The transition models answer *which* outcome a loan reaches; these answer *how
much*, given that it did.

```bash
python -m run.train_models --stage amounts --name transitions_full
```

| model | target | observations |
|---|---|---|
| severity | `LOSS_SEVERITY`, conditioned on WHICH credit event | 19,630 |
| prepay hazard | `IS_PARTIAL_PREPAYMENT` | 3.5M sampled of 71M reliable |
| curtailment | `CURTAILMENT / PRIOR_UPB` | 1.5M sampled of 15.2M |

There is deliberately **no full-prepayment amount model**:
`ZERO_BALANCE_REMOVAL / PRIOR_UPB` is exactly 1.0000 at the 10th, 50th and 90th
percentiles, so fitting it would estimate a constant. That identity also confirms
the severity denominator is `PRIOR_UPB`, which is what makes
`loss = balance x severity` the right arithmetic.

**Quantile regression, not a conditional mean.** Monte Carlo needs a
distribution, and realised severity runs from −7.9% at the 1st percentile (a
disposition can produce a gain) to +147.8% at the 99th. Nine boosters per target
are fitted across the grid and sampled by inverting the predicted curve, so the
*shape* of the distribution varies with covariates. Coverage on held-out data —
the only test that establishes a quantile model is a distribution rather than a
curve that fits — comes out at **worst error 0.042, and 0.040 out-of-time**.

Conditioning on the outcome is the single biggest gain available, and it works:
predicted median severity by destination is 0.542 / 0.380 / 0.362 against actual
0.540 / 0.368 / 0.335, instead of collapsing to the pooled 0.453.

Independently fitted quantiles cross — on **29% of real rows** — which would
yield a non-monotone inverse CDF and therefore invalid draws. Each predicted
curve is sorted before use.

### Balances became a path variable

Simulated balances used to follow the closed-form origination schedule. They now
carry forward per path, amortising through `scheduled_principal` on the loan's
*own* balance and scaled by how much of the scheduled principal a loan in that
delinquency state actually pays. Read off the observed medians:

| entering the month | principal paid |
|---|---|
| current | 0.202% of balance |
| 1 month down | 0.152% |
| 2 months down | 0.088% |
| **90+ days down** | **0.000%** |

A delinquent loan stops paying down, which is why defaulted loans really do carry
balances a median 2.4% *above* the origination schedule (p90 +9.8%). Encoding it
this way reproduces that mechanically, with no extra model.

Curtailment then reduces the balance where the hazard fires. It matters:
curtailment is **32.7% of all principal reduction** ($14.4bn against $29.5bn
scheduled), and simulating it takes prepayment speed from structurally absent to
**mean CPR 19.08% against an actual 17.81%**, with total curtailment $260M
against an actual $298M.

### A data artefact the severity ratio hides

Nine credit events carry a defaulted balance of $0.01–$517 against a median of
$141,286 — loans already paid down to nothing — producing severities up to
**657,341**. In dollars they are 0.005% of all loss, but the bucketed sampler
draws directly from the observed array, so one draw would book a phantom loss
larger than the portfolio. `severity.py` guards only `ZERO_BALANCE_REMOVAL > 0`;
both samplers now require a materially positive balance.

### 2007 backtest under realised macro

All 50,000 real loans of the vintage, 120 months, macro path as it actually
occurred -- so any remaining gap is model error, not scenario error.

| | credit events | loss (% of original UPB) |
|---|---|---|
| matrix projection (cohort-average loan) | 1.32% | ~0% |
| Monte Carlo, flat/bucketed amounts | 5.02% | 2.34% |
| **Monte Carlo + conditional amount models** | **4.74%** | **1.92%** |
| actual, within 120 months | 8.35% | 3.76% |

**3.6x the matrix on credit events, closing roughly half the distance to actual.**
Loss per event lands at 0.405 against an actual 0.450 -- so severity is ~90% of
realised, and the dominant remaining gap is the event COUNT, not the amount. The
run takes 6.5 seconds without the amount models and 89 seconds with them; the
quantile grid costs nine boosters per curtailment draw, every month.

The residual is now located, and it is *not* the charge-off transition:
simulated delinquency peaks at 10.40% against an actual 15.50%, and runs at
0.45-0.58x actual from 2010 onward. Too few loans become delinquent and stay
delinquent; the ones that do get resolved about right.

The largest identified contributor is `PRIOR_IS_MODIFIED`, which simulation
freezes. Modification is a servicer loss-mitigation decision that nothing here
predicts, and inventing modifications would fabricate the most effective cure
mechanism of the crisis vintages -- but the flag matters: modified loan-months
run a **24.8% delinquency rate against 7.9% unmodified, 3.1x**, and 6.16% of the
2007 vintage was modified at some point. That accounts for roughly a quarter of
the remaining gap. Modelling modification as its own transition is the next
lever, ahead of any further feature work.

Treat prepayment as trustworthy, and simulated credit losses as a much better
floor than the matrix gave -- still a floor.

### A leakage bug found by the parity gate

`MONTHS_SINCE_FIRST_DLQ` ranked first delinquency by `min(LOAN_AGE)`. Taking a
minimum over the whole loan partition is only causally safe if the key is
monotonic -- and `LOAN_AGE` is not, because a modification resets it. Loan
`F07Q10169987` is modified in 200911 while 30 days down and its age resets 32 ->
1, so the feature reported "1 month since first delinquency" back in **200704,
five months before the loan first missed a payment**.

That affected **234,852 modelable loan-months -- 2.55% of the rows where the
feature was populated**, concentrated in modified loans, which are exactly the
crisis-vintage loans that drive credit losses. The feature is now measured on the
reporting period, which is monotonic by construction; the rebuilt panel contains
zero leaking rows.

It surfaced only because the simulator physically cannot reproduce a non-causal
value, so the parity test had nowhere to hide it.

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