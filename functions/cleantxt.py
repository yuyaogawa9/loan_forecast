import os
import pandas as pd
import concurrent.futures


class FreddieMacLoader:

    """Loader for Freddie Mac Single-Family Loan Performance Data.
    This class reads the original and performance data files from Freddie Mac's dataset,
    processes them, and returns a pandas DataFrame.
    Args:
        base_path (str): The base path where the Freddie Mac data files are stored.
        data_type (str): Type of data to load, either 'orig' for original data or 'perf' for performance data.
        parallel (bool): Whether to load data in parallel. Defaults to True.
    Raises:
        ValueError: If data_type is not 'orig' or 'perf'.
    """
    
    def __init__(self, base_path, data_type='orig', parallel=True):
        if data_type not in ['orig', 'perf']:
            raise ValueError("data_type must be 'orig' or 'perf'")
        self.base_path = base_path
        self.data_type = data_type
        self.parallel = parallel
        self.columns = self._get_columns(data_type)
    
    def _get_columns(self, data_type):
        if data_type == 'orig':
            return ['CREDIT_SCORE', 'FIRST_PAYMENT_DATE', 'FIRST_TIME_HOMEBUYER_FLAG', 'MATURITY_DATE', 'METROPOLITAN_DIVISION',
                    'MORTGAGE_INSURANCE_PERCENTAGE', 'NUMBER_OF_UNITS', 'OCCUPANCY_STATUS', 'ORIGINAL_COMBINED_LOAN_TO_VALUE',
                    'ORIGINAL_DEBT_TO_INCOME', 'ORIGINAL_UPB', 'ORIGINAL_LOAN_TO_VALUE', 'ORIGINAL_INTEREST_RATE', 'CHANNEL',
                    'PREPAYMENT_PENALTY_MORTGAGE', 'AMORTIZATION_TYPE', 'PROPERTY_STATE', 'PROPERTY_TYPE', 'POSTAL_CODE',
                    'LOAN_SEQUENCE_NUMBER', 'LOAN_PURPOSE', 'ORIGINAL_LOAN_TERM', 'NUMBER_OF_BORROWERS', 'SELLER_NAME',
                    'SERVICER_NAME', 'SUPER_CONFORMING_FLAG', 'PRE_RELIEF_REFINANCE_LOAN_SEQUENCE_NUMBER', 'PROGRAM_INDICATOR',
                    'RELIEF_REFINANCE_INDICATOR', 'PROPERTY_VALUATION_METHOD', 'INTEREST_ONLY_INDICATOR', 'MI_CANCELLATION_INDICATOR']
        else:  # perf
            return ['LOAN_SEQUENCE_NUMBER', 'MONTHLY_REPORTING_PERIOD', 'CURRENT_ACTUAL_UPB', 'CURRENT_LOAN_DELINQUENCY_STATUS', 'LOAN_AGE',
                    'REMAINING_MONTHS_TO_LEGAL_MATURITY', 'DEFECT_SETTLEMENT_DATE', 'MODIFICATION_FLAG', 'ZERO_BALANCE_CODE',
                    'ZERO_BALANCE_EFFECTIVE_DATE', 'CURRENT_INTEREST_RATE', 'CURRENT_NON_INTEREST_BEARING_UPB', 'DUE_DATE_OF_LAST_PAID_INSTALLMENT', 
                    'MI_RECOVERIES', 'NET_SALE_PROCEEDS', 'NON_MI_RECOVERIES', 'TOTAL_EXPENSES', 'LEGAL_COSTS', 'MAINTENANCE_AND_PRESERVATION_COSTS',
                    'TAXES_AND_INSURANCE', 'MISCELLANEOUS_EXPENSES', 'ACTUAL_LOSS_CALCULATION', 'CUMULATIVE_MODIFICATION_COST', 'STEP_MODIFICATION_FLAG', 
                    'PAYMENT_DEFERRAL', 'ESTIMATED_LOAN_TO_VALUE', 'ZERO_BALANCE_REMOVAL', 'DELINQUENT_ACCRUED_INTEREST',
                    'DELINQUENCY_DUE_TO_DISASTER', 'BORROWER_ASSISTANCE_STATUS_CODE', 'CURRENT_MONTH_MODIFICATION_COST', 'INTEREST_BEARING_UPB']

    def _read_file(self, year):
        suffix = 'orig' if self.data_type == 'orig' else 'svcg'
        filename = os.path.join(self.base_path, f"sample_{year}", f"sample_{suffix}_{year}.txt")
        data = []
        with open(filename, 'r') as file:
            for line in file:
                fields = [field.strip() for field in line.strip().split('|')]

                if len(fields) == len(self.columns):
                    data.append(fields)
                else:
                    print(f"[{year}] Skipped line with {len(fields)} fields: {line.strip()[:50]}...")
        return data

    def load(self, years):
        all_data = []
        if self.parallel and len(years) > 2:
            print("🔄 Loading in parallel...")
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {executor.submit(self._read_file, year): year for year in years}
                for future in concurrent.futures.as_completed(futures):
                    year = futures[future]
                    try:
                        year_data = future.result()
                        all_data.extend(year_data)
                        print(f"✅ {year}: {len(year_data)} rows loaded")
                    except Exception as e:
                        print(f"❌ {year}: Failed with error: {e}")
        else:
            print("🔄 Loading sequentially...")
            for year in years:
                year_data = self._read_file(year)
                all_data.extend(year_data)
                print(f"✅ {year}: {len(year_data)} rows loaded")

        df = pd.DataFrame(all_data, columns=self.columns)

        if self.data_type == "perf":
            cols_idx = [3,5,6,9,11,12,14,16,17,18,19,20,21,22,23,26,27,28,31,32]
        
        elif self.data_type == "orig":
            cols_idx = [1, 5, 6, 7, 9, 10, 11, 12, 13, 19, 22, 23, 30]

        df.iloc[:, cols_idx] = df.iloc[:, cols_idx].apply(pd.to_numeric, errors='coerce')

        return df