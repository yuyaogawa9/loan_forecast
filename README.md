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