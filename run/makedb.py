import gc
import duckdb
from fredapi import Fred
import pandas as pd
from functions.cleantxt import FreddieMacLoader
from functions.todb import exportDuckDB


# Path to DuckDB file
basepath = "set where your data is stored"
db_path = f"{basepath}/fraddie_mae.duckdb"
fredAPIKey = "Put Fred API key here"

# Load Freddie Mac data from origination files
loader = FreddieMacLoader(
    base_path=basepath,
    data_type='orig',
    parallel=True
)

orig = loader.load([year for year in range(1999,2024)])

exportDuckDB(df=orig, 
             table_name='origination', 
             file_name='fraddie_mae.duckdb', 
             base_path=basepath)
gc.collect()


loader = FreddieMacLoader(
    base_path=basepath,
    data_type='perf',
    parallel=True
)

for year in range(2002, 2028)[::3]:  # Load in chunks to avoid memory issues
    perf = loader.load([year for year in range(year-3,year)])
    exportDuckDB(df=perf, 
                table_name='performance', 
                file_name='fraddie_mae.duckdb', 
                base_path=basepath)
    # remove perf from memory to free up space
    del perf
    gc.collect()


# Convert to Parquet file for lazy processing later on 
conn = duckdb.connect(db_path)
# Export origination table to Parquet in fraddie_mae_data folder
conn.execute(f"""
    COPY (SELECT * FROM origination) TO '{basepath}/origination.parquet' (FORMAT PARQUET)
""")
# Export performance table to Parquet in fraddie_mae_data folder
conn.execute(f"""
    COPY (SELECT * FROM performance) TO '{basepath}/performance.parquet' (FORMAT PARQUET)
""")


# Make a state level unemployment rate data
fred = Fred(api_key=fredAPIKey)

states = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]

def prep_unemp(state):
    series = fred.get_series(f"{state}UR")
    df = series.reset_index()
    df.columns = ["date", "unemp"]
    df['state'] = state
    df['date'] = df["date"].dt.year * 100 + df["date"].dt.month
    return df

# Apply to all states
unemp_data = pd.concat(list(map(prep_unemp, states)), ignore_index=True)

unemp_data.to_parquet(f"{basepath}/state_unemp.parquet", index=False)
