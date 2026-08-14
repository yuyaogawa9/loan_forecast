"""Schema, sentinel and ingest behaviour.

Each test here corresponds to a specific way the previous loader lost or
corrupted data.
"""

from __future__ import annotations

import polars as pl
import pytest

from loan_etl.clean.ingest import ingest_vintage
from loan_etl.io import scan_dataset
from loan_etl.schema import SchemaError, check_arity, load_schema

from .conftest import VINTAGE


# --- schema spec -----------------------------------------------------------


@pytest.mark.parametrize("dataset", ["origination", "performance"])
def test_schema_loads_with_contiguous_positions(dataset):
    s = load_schema(dataset, 47)
    assert s.field_count == 32
    assert [f.pos for f in s.fields] == list(range(1, 33))
    assert len(set(s.names)) == 32


def test_unknown_schema_version_raises():
    with pytest.raises(SchemaError, match="No schema at"):
        load_schema("origination", 999)


def test_arity_mismatch_raises_instead_of_dropping_rows(tmp_path):
    """The previous loader silently discarded every row on a layout mismatch."""
    bad = tmp_path / "bad.txt"
    bad.write_text("a|b|c\n" * 5)  # 3 fields, schema expects 32
    with pytest.raises(SchemaError, match="layout-version mismatch"):
        check_arity(bad, load_schema("origination", 47))


# --- sentinels -------------------------------------------------------------


def test_sentinels_become_null(bronze):
    o = (
        scan_dataset(bronze.bronze / "origination")
        .filter(pl.col("LOAN_SEQUENCE_NUMBER") == "F07Q10000002")
        .collect()
    )
    assert o.height == 1
    row = o.to_dicts()[0]
    for col in (
        "CREDIT_SCORE",
        "ORIGINAL_DEBT_TO_INCOME",
        "ORIGINAL_LOAN_TO_VALUE",
        "ORIGINAL_COMBINED_LOAN_TO_VALUE",
        "MORTGAGE_INSURANCE_PERCENTAGE",
        "NUMBER_OF_UNITS",
        "POSTAL_CODE",
    ):
        assert row[col] is None, f"{col} sentinel survived as {row[col]!r}"


def test_credit_score_never_exceeds_850(bronze):
    """The single clearest before/after check: this was 9999 previously."""
    o = scan_dataset(bronze.bronze / "origination").collect()
    assert o["CREDIT_SCORE"].max() <= 850


def test_zero_mi_percentage_is_kept_not_nulled(bronze):
    """000 means 'no mortgage insurance' -- a real value, not a sentinel."""
    o = (
        scan_dataset(bronze.bronze / "origination")
        .filter(pl.col("LOAN_SEQUENCE_NUMBER") == "F07Q10000001")
        .collect()
    )
    assert o["MORTGAGE_INSURANCE_PERCENTAGE"][0] == 0


def test_blank_dti_is_null(bronze):
    o = (
        scan_dataset(bronze.bronze / "origination")
        .filter(pl.col("LOAN_SEQUENCE_NUMBER") == "F07Q10000006")
        .collect()
    )
    assert o["ORIGINAL_DEBT_TO_INCOME"][0] is None


# --- mixed-type and categorical fields -------------------------------------


def test_net_sale_proceeds_codes_are_preserved_and_split(bronze):
    p = scan_dataset(bronze.bronze / "performance").collect()
    codes = p.filter(pl.col("NET_SALE_PROCEEDS_CODE").is_not_null())
    assert set(codes["NET_SALE_PROCEEDS_CODE"]) == {"C", "U"}
    # A coded row has no numeric amount, and a numeric row has no code.
    assert codes["NET_SALE_PROCEEDS_AMT"].null_count() == codes.height
    numeric = p.filter(pl.col("NET_SALE_PROCEEDS_AMT").is_not_null())
    assert numeric["NET_SALE_PROCEEDS_AMT"].to_list() == [150000.0]
    assert numeric["NET_SALE_PROCEEDS_CODE"].null_count() == numeric.height


def test_delinquency_status_stays_categorical(bronze):
    """RA must survive; XX is a declared sentinel and becomes null."""
    p = scan_dataset(bronze.bronze / "performance").collect()
    statuses = set(p["CURRENT_LOAN_DELINQUENCY_STATUS"].drop_nulls())
    assert "RA" in statuses
    assert "XX" not in statuses
    assert p["CURRENT_LOAN_DELINQUENCY_STATUS"].null_count() >= 1


def test_yyyymm_columns_have_both_string_and_date_forms(bronze):
    p = scan_dataset(bronze.bronze / "performance").collect()
    assert p.schema["MONTHLY_REPORTING_PERIOD"] == pl.Utf8
    assert p.schema["MONTHLY_REPORTING_PERIOD_DT"] == pl.Date
    row = p.sort("MONTHLY_REPORTING_PERIOD").row(0, named=True)
    assert row["MONTHLY_REPORTING_PERIOD"] == "200703"
    assert str(row["MONTHLY_REPORTING_PERIOD_DT"]) == "2007-03-01"


# --- structural handling ---------------------------------------------------


def test_malformed_row_is_quarantined_not_dropped(bronze):
    manifest_rows = 0
    o = scan_dataset(bronze.bronze / "origination").collect()
    assert "NOT-A-LOAN-ID" not in set(o["LOAN_SEQUENCE_NUMBER"])

    q = bronze.quarantine / "origination" / f"vintage_year={VINTAGE}" / "rejected.parquet"
    assert q.exists(), "rejected row was dropped instead of quarantined"
    rejected = pl.read_parquet(q)
    manifest_rows = rejected.height
    assert manifest_rows == 1
    assert rejected["LOAN_SEQUENCE_NUMBER"][0].strip() == "NOT-A-LOAN-ID"


def test_row_accounting_balances(bronze):
    from loan_etl.io import read_manifest

    m = read_manifest(bronze.manifests, f"bronze__origination__{VINTAGE}")
    r = m["rows"]
    assert r["written"] + r["quarantined"] == r["parsed"] == r["source_lines"]


def test_vintage_decoded_from_loan_id(bronze):
    p = scan_dataset(bronze.bronze / "performance").collect()
    assert set(p["LOAN_VINTAGE_YEAR"]) == {VINTAGE}
    assert set(p["LOAN_VINTAGE_QUARTER"]) == {1}


def test_ingest_is_idempotent(bronze):
    """Re-running with an unchanged source must skip, not rebuild."""
    m = ingest_vintage(bronze, "origination", VINTAGE, keep_extracted=True)
    assert m["skipped"] is True

    forced = ingest_vintage(bronze, "origination", VINTAGE, force=True, keep_extracted=True)
    assert forced["skipped"] is False