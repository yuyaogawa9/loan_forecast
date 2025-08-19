import os
import pandas as pd
import duckdb


def exportDuckDB(df, table_name, file_name, base_path=None):
    """
    Exports the DataFrame to a DuckDB table.

    Parameters:
    - df: The DataFrame to export.
    - table_name: The name of the DuckDB table (origination or performance).
    - conn: The DuckDB connection object.
    """

    if not file_name.endswith('.duckdb'):
        file_name += '.duckdb'

    if table_name not in ['origination', 'performance']:
        raise ValueError("table_name must be either 'origination' or 'performance'")
    
    file_name = os.path.join(base_path,file_name)
    conn = duckdb.connect(database=file_name, read_only=False)
    conn.register('temp_df', df)  # Register the DataFrame as a temporary DuckDB table
        # if table exists, insert only if there is no duplicate loan_sequence_number
    if conn.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()[0] > 0:
        print(f"Table '{table_name}' already exists. Inserting data...")
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
    else:
        print(f"Table '{table_name}' does not exist. Creating and inserting data...")
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")

    conn.unregister('temp_df')
    conn.close()
    print(f"✅ Data exported to DuckDB table '{table_name}' successfully.")