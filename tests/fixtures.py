"""Synthetic Freddie Mac source files covering the edge cases that matter.

Deliberately includes every trap the previous loader fell into: sentinel values
(9999 credit score, 999 DTI/LTV, 00000 postal code), non-numeric delinquency
codes (RA, XX), the C/U codes in NET_SALE_PROCEEDS, a full loss/recovery
disposition, and a structurally malformed row that must be quarantined rather
than silently dropped.
"""

from __future__ import annotations

from pathlib import Path

SEP = "|"

# Origination field order, v47. Index = position - 1.
ORIG_TEMPLATE = [
    "720",          # 1  CREDIT_SCORE
    "200703",       # 2  FIRST_PAYMENT_DATE
    "N",            # 3  FIRST_TIME_HOMEBUYER_FLAG
    "203702",       # 4  MATURITY_DATE
    "16974",        # 5  METROPOLITAN_DIVISION
    "000",          # 6  MORTGAGE_INSURANCE_PERCENTAGE  (000 = no MI, a REAL value)
    "1",            # 7  NUMBER_OF_UNITS
    "P",            # 8  OCCUPANCY_STATUS
    "80",           # 9  ORIGINAL_COMBINED_LOAN_TO_VALUE
    "35",           # 10 ORIGINAL_DEBT_TO_INCOME
    "200000",       # 11 ORIGINAL_UPB
    "80",           # 12 ORIGINAL_LOAN_TO_VALUE
    "6.250",        # 13 ORIGINAL_INTEREST_RATE
    "R",            # 14 CHANNEL
    "N",            # 15 PREPAYMENT_PENALTY_MORTGAGE
    "FRM",          # 16 AMORTIZATION_TYPE
    "IL",           # 17 PROPERTY_STATE
    "SF",           # 18 PROPERTY_TYPE
    "60600",        # 19 POSTAL_CODE
    "F07Q10000001", # 20 LOAN_SEQUENCE_NUMBER
    "P",            # 21 LOAN_PURPOSE
    "360",          # 22 ORIGINAL_LOAN_TERM
    "2",            # 23 NUMBER_OF_BORROWERS
    "Other sellers",# 24 SELLER_NAME
    "Other servicers",  # 25 SERVICER_NAME
    "",             # 26 SUPER_CONFORMING_FLAG
    "",             # 27 PRE_RELIEF_REFINANCE_LOAN_SEQUENCE_NUMBER
    "9",            # 28 PROGRAM_INDICATOR
    "",             # 29 RELIEF_REFINANCE_INDICATOR
    "9",            # 30 PROPERTY_VALUATION_METHOD
    "N",            # 31 INTEREST_ONLY_INDICATOR
    "N",            # 32 MI_CANCELLATION_INDICATOR
]

# Performance field order, v47.
PERF_TEMPLATE = [
    "F07Q10000001", # 1  LOAN_SEQUENCE_NUMBER
    "200703",       # 2  MONTHLY_REPORTING_PERIOD
    "200000.00",    # 3  CURRENT_ACTUAL_UPB
    "0",            # 4  CURRENT_LOAN_DELINQUENCY_STATUS
    "0",            # 5  LOAN_AGE
    "360",          # 6  REMAINING_MONTHS_TO_LEGAL_MATURITY
    "",             # 7  DEFECT_SETTLEMENT_DATE
    "",             # 8  MODIFICATION_FLAG
    "",             # 9  ZERO_BALANCE_CODE
    "",             # 10 ZERO_BALANCE_EFFECTIVE_DATE
    "6.250",        # 11 CURRENT_INTEREST_RATE
    "0.00",         # 12 CURRENT_NON_INTEREST_BEARING_UPB
    "200702",       # 13 DUE_DATE_OF_LAST_PAID_INSTALLMENT
    "",             # 14 MI_RECOVERIES
    "",             # 15 NET_SALE_PROCEEDS
    "",             # 16 NON_MI_RECOVERIES
    "",             # 17 TOTAL_EXPENSES
    "",             # 18 LEGAL_COSTS
    "",             # 19 MAINTENANCE_AND_PRESERVATION_COSTS
    "",             # 20 TAXES_AND_INSURANCE
    "",             # 21 MISCELLANEOUS_EXPENSES
    "",             # 22 ACTUAL_LOSS_CALCULATION
    "",             # 23 CUMULATIVE_MODIFICATION_COST
    "N",            # 24 STEP_MODIFICATION_FLAG
    "",             # 25 PAYMENT_DEFERRAL
    "80",           # 26 ESTIMATED_LOAN_TO_VALUE
    "",             # 27 ZERO_BALANCE_REMOVAL
    "",             # 28 DELINQUENT_ACCRUED_INTEREST
    "",             # 29 DELINQUENCY_DUE_TO_DISASTER
    "",             # 30 BORROWER_ASSISTANCE_STATUS_CODE
    "",             # 31 CURRENT_MONTH_MODIFICATION_COST
    "200000.00",    # 32 INTEREST_BEARING_UPB
]

O = {name: i for i, name in enumerate([
    "CREDIT_SCORE", "FIRST_PAYMENT_DATE", "FIRST_TIME_HOMEBUYER_FLAG", "MATURITY_DATE",
    "METROPOLITAN_DIVISION", "MORTGAGE_INSURANCE_PERCENTAGE", "NUMBER_OF_UNITS",
    "OCCUPANCY_STATUS", "ORIGINAL_COMBINED_LOAN_TO_VALUE", "ORIGINAL_DEBT_TO_INCOME",
    "ORIGINAL_UPB", "ORIGINAL_LOAN_TO_VALUE", "ORIGINAL_INTEREST_RATE", "CHANNEL",
    "PREPAYMENT_PENALTY_MORTGAGE", "AMORTIZATION_TYPE", "PROPERTY_STATE", "PROPERTY_TYPE",
    "POSTAL_CODE", "LOAN_SEQUENCE_NUMBER", "LOAN_PURPOSE", "ORIGINAL_LOAN_TERM",
    "NUMBER_OF_BORROWERS", "SELLER_NAME", "SERVICER_NAME", "SUPER_CONFORMING_FLAG",
    "PRE_RELIEF_REFINANCE_LOAN_SEQUENCE_NUMBER", "PROGRAM_INDICATOR",
    "RELIEF_REFINANCE_INDICATOR", "PROPERTY_VALUATION_METHOD", "INTEREST_ONLY_INDICATOR",
    "MI_CANCELLATION_INDICATOR",
])}

P = {name: i for i, name in enumerate([
    "LOAN_SEQUENCE_NUMBER", "MONTHLY_REPORTING_PERIOD", "CURRENT_ACTUAL_UPB",
    "CURRENT_LOAN_DELINQUENCY_STATUS", "LOAN_AGE", "REMAINING_MONTHS_TO_LEGAL_MATURITY",
    "DEFECT_SETTLEMENT_DATE", "MODIFICATION_FLAG", "ZERO_BALANCE_CODE",
    "ZERO_BALANCE_EFFECTIVE_DATE", "CURRENT_INTEREST_RATE",
    "CURRENT_NON_INTEREST_BEARING_UPB", "DUE_DATE_OF_LAST_PAID_INSTALLMENT",
    "MI_RECOVERIES", "NET_SALE_PROCEEDS", "NON_MI_RECOVERIES", "TOTAL_EXPENSES",
    "LEGAL_COSTS", "MAINTENANCE_AND_PRESERVATION_COSTS", "TAXES_AND_INSURANCE",
    "MISCELLANEOUS_EXPENSES", "ACTUAL_LOSS_CALCULATION", "CUMULATIVE_MODIFICATION_COST",
    "STEP_MODIFICATION_FLAG", "PAYMENT_DEFERRAL", "ESTIMATED_LOAN_TO_VALUE",
    "ZERO_BALANCE_REMOVAL", "DELINQUENT_ACCRUED_INTEREST", "DELINQUENCY_DUE_TO_DISASTER",
    "BORROWER_ASSISTANCE_STATUS_CODE", "CURRENT_MONTH_MODIFICATION_COST",
    "INTEREST_BEARING_UPB",
])}


def _orig(**kw: str) -> str:
    row = list(ORIG_TEMPLATE)
    for k, v in kw.items():
        row[O[k]] = v
    return SEP.join(row)


def _perf(**kw: str) -> str:
    row = list(PERF_TEMPLATE)
    for k, v in kw.items():
        row[P[k]] = v
    return SEP.join(row)


def _months(start: str, n: int) -> list[str]:
    y, m = int(start[:4]), int(start[4:])
    out = []
    for _ in range(n):
        out.append(f"{y}{m:02d}")
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


def build_origination_lines() -> list[str]:
    return [
        # L1 -- clean, prepays later.
        _orig(LOAN_SEQUENCE_NUMBER="F07Q10000001"),
        # L2 -- every numeric sentinel at once. CREDIT_SCORE 9999, DTI 999,
        # LTV/CLTV 999, MI% 999, UNITS 99, POSTAL 00000. All must become null.
        _orig(
            LOAN_SEQUENCE_NUMBER="F07Q10000002",
            CREDIT_SCORE="9999",
            ORIGINAL_DEBT_TO_INCOME="999",
            ORIGINAL_LOAN_TO_VALUE="999",
            ORIGINAL_COMBINED_LOAN_TO_VALUE="999",
            MORTGAGE_INSURANCE_PERCENTAGE="999",
            NUMBER_OF_UNITS="99",
            POSTAL_CODE="00000",
            FIRST_TIME_HOMEBUYER_FLAG="9",
            OCCUPANCY_STATUS="9",
            CHANNEL="9",
            PROPERTY_STATE="DC",  # exercises the state FRED omitted
        ),
        # L3 -- defaults to REO disposition with a real loss.
        _orig(LOAN_SEQUENCE_NUMBER="F07Q10000003", CREDIT_SCORE="640", PROPERTY_STATE="CA"),
        # L4 -- charge-off, NET_SALE_PROCEEDS = "C".
        _orig(LOAN_SEQUENCE_NUMBER="F07Q10000004", CREDIT_SCORE="610", PROPERTY_STATE="FL"),
        # L5 -- REO with NET_SALE_PROCEEDS = "U" and an RA delinquency status.
        _orig(LOAN_SEQUENCE_NUMBER="F07Q10000005", CREDIT_SCORE="580", PROPERTY_STATE="NV"),
        # L6 -- blank DTI (Freddie encodes DTI > 65% this way in newer releases).
        _orig(LOAN_SEQUENCE_NUMBER="F07Q10000006", ORIGINAL_DEBT_TO_INCOME=""),
        # L7 -- STRUCTURALLY BROKEN loan id. Must land in quarantine, not be
        # dropped silently and not corrupt the join.
        _orig(LOAN_SEQUENCE_NUMBER="NOT-A-LOAN-ID"),
    ]


def build_performance_lines() -> list[str]:
    lines: list[str] = []

    def amortize(upb: float, rate: float, term: int, n: int) -> list[float]:
        i = rate / 1200.0
        pmt = upb * i / (1 - (1 + i) ** -term)
        bal, out = upb, []
        for _ in range(n):
            interest = bal * i
            bal = bal - (pmt - interest)
            out.append(round(bal, 2))
        return out

    # --- L1: 6 clean months then voluntary prepayment (ZB 01) --------------
    bals = amortize(200000, 6.25, 360, 6)
    for k, (period, bal) in enumerate(zip(_months("200703", 6), bals)):
        last = k == 5
        lines.append(_perf(
            LOAN_SEQUENCE_NUMBER="F07Q10000001", MONTHLY_REPORTING_PERIOD=period,
            CURRENT_ACTUAL_UPB=f"{bal:.2f}", INTEREST_BEARING_UPB=f"{bal:.2f}",
            LOAN_AGE=str(k), REMAINING_MONTHS_TO_LEGAL_MATURITY=str(360 - k - 1),
            ZERO_BALANCE_CODE="01" if last else "",
            ZERO_BALANCE_EFFECTIVE_DATE=period if last else "",
            ZERO_BALANCE_REMOVAL=f"{bal:.2f}" if last else "",
        ))

    # --- L2: 4 months, one with an unknown delinquency status "XX" ---------
    for k, period in enumerate(_months("200703", 4)):
        lines.append(_perf(
            LOAN_SEQUENCE_NUMBER="F07Q10000002", MONTHLY_REPORTING_PERIOD=period,
            CURRENT_ACTUAL_UPB="199000.00", INTEREST_BEARING_UPB="199000.00",
            LOAN_AGE=str(k), CURRENT_LOAN_DELINQUENCY_STATUS="XX" if k == 2 else "0",
            ESTIMATED_LOAN_TO_VALUE="999" if k == 3 else "80",
        ))

    # --- L3: rolls 0->1->2->3->6, then REO disposition with a loss ---------
    statuses = ["0", "1", "2", "3", "4", "5", "6", "RA"]
    for k, (period, dlq) in enumerate(zip(_months("200703", 8), statuses)):
        terminal = k == 7
        lines.append(_perf(
            LOAN_SEQUENCE_NUMBER="F07Q10000003", MONTHLY_REPORTING_PERIOD=period,
            CURRENT_ACTUAL_UPB="198500.00", INTEREST_BEARING_UPB="198500.00",
            LOAN_AGE=str(k), CURRENT_LOAN_DELINQUENCY_STATUS=dlq,
            ZERO_BALANCE_CODE="09" if terminal else "",
            ZERO_BALANCE_EFFECTIVE_DATE=period if terminal else "",
            ZERO_BALANCE_REMOVAL="198500.00" if terminal else "",
            NET_SALE_PROCEEDS="150000.00" if terminal else "",
            MI_RECOVERIES="20000.00" if terminal else "",
            NON_MI_RECOVERIES="1000.00" if terminal else "",
            TOTAL_EXPENSES="-12000.00" if terminal else "",
            LEGAL_COSTS="-3000.00" if terminal else "",
            MAINTENANCE_AND_PRESERVATION_COSTS="-4000.00" if terminal else "",
            TAXES_AND_INSURANCE="-4000.00" if terminal else "",
            MISCELLANEOUS_EXPENSES="-1000.00" if terminal else "",
            DELINQUENT_ACCRUED_INTEREST="8000.00" if terminal else "",
            ACTUAL_LOSS_CALCULATION="-47500.00" if terminal else "",
        ))

    # --- L4: charge-off (ZB 03) with NET_SALE_PROCEEDS = "C" ---------------
    for k, period in enumerate(_months("200703", 3)):
        terminal = k == 2
        lines.append(_perf(
            LOAN_SEQUENCE_NUMBER="F07Q10000004", MONTHLY_REPORTING_PERIOD=period,
            CURRENT_ACTUAL_UPB="197000.00", INTEREST_BEARING_UPB="197000.00",
            LOAN_AGE=str(k), CURRENT_LOAN_DELINQUENCY_STATUS=str(min(k, 3)),
            ZERO_BALANCE_CODE="03" if terminal else "",
            ZERO_BALANCE_EFFECTIVE_DATE=period if terminal else "",
            ZERO_BALANCE_REMOVAL="197000.00" if terminal else "",
            NET_SALE_PROCEEDS="C" if terminal else "",
            ACTUAL_LOSS_CALCULATION="-15000.00" if terminal else "",
        ))

    # --- L5: REO with NET_SALE_PROCEEDS = "U" (unknown) ---------------------
    for k, period in enumerate(_months("200703", 3)):
        terminal = k == 2
        lines.append(_perf(
            LOAN_SEQUENCE_NUMBER="F07Q10000005", MONTHLY_REPORTING_PERIOD=period,
            CURRENT_ACTUAL_UPB="196000.00", INTEREST_BEARING_UPB="196000.00",
            LOAN_AGE=str(k), CURRENT_LOAN_DELINQUENCY_STATUS="RA" if terminal else "3",
            ZERO_BALANCE_CODE="09" if terminal else "",
            ZERO_BALANCE_EFFECTIVE_DATE=period if terminal else "",
            ZERO_BALANCE_REMOVAL="196000.00" if terminal else "",
            NET_SALE_PROCEEDS="U" if terminal else "",
            ACTUAL_LOSS_CALCULATION="-30000.00" if terminal else "",
        ))

    # --- L6: partial prepayment (curtailment) at month 3 --------------------
    bals = amortize(200000, 6.25, 360, 5)
    for k, (period, bal) in enumerate(zip(_months("200703", 5), bals)):
        actual = bal - 10000.0 if k >= 3 else bal   # $10k curtailment in month 3
        lines.append(_perf(
            LOAN_SEQUENCE_NUMBER="F07Q10000006", MONTHLY_REPORTING_PERIOD=period,
            CURRENT_ACTUAL_UPB=f"{actual:.2f}", INTEREST_BEARING_UPB=f"{actual:.2f}",
            LOAN_AGE=str(k), REMAINING_MONTHS_TO_LEGAL_MATURITY=str(360 - k - 1),
        ))

    return lines


def write_vintage(raw_dir: Path, year: int = 2007) -> tuple[Path, Path]:
    """Materialise a synthetic vintage in the on-disk layout ingest expects."""
    vdir = raw_dir / f"sample_{year}"
    vdir.mkdir(parents=True, exist_ok=True)
    orig = vdir / f"sample_orig_{year}.txt"
    perf = vdir / f"sample_svcg_{year}.txt"
    orig.write_text("\n".join(build_origination_lines()) + "\n")
    perf.write_text("\n".join(build_performance_lines()) + "\n")
    return orig, perf