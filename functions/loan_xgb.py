import polars as pl
import xgboost as xgb
import pandas as pd
import numpy as np
import duckdb

def train_bootstrap(loan_df, unique_loans, features, target, 
                    sample_frac=0.01, n_estimators=50, max_depth=3, learning_rate = 0.1,
                    subsample = 0.8,colsample_bytree=0.8):
    # Sample ~sample_frac of unique loans
    sampled_loans = np.random.choice(unique_loans, size=int(sample_frac*len(unique_loans)), replace=True)
    
    # Filter lazily by sampled loans
    sample_df = loan_df.filter(pl.col("LOAN_SEQUENCE_NUMBER").is_in(sampled_loans)).collect()
    
    # Convert to pandas for XGBoost
    sample_pd = sample_df.to_pandas()
    
    X = sample_pd[features]
    y = sample_pd[target]
    
    model = xgb.XGBClassifier(
        n_estimators=n_estimators,
        max_depth=max_depth,
        learning_rate=learning_rate,
        subsample=subsample,
        colsample_bytree=colsample_bytree,
        eval_metric="logloss"
    )
    
    model.fit(X, y)
    
    del sample_df  # free memory
    return model


def process_chunk(offset, length, val_df, xgb_models, features, duckdb_file, table_name):
    # Slice chunk lazily and collect
    chunk = val_df.slice(offset, length).collect().to_pandas()
    
    # Predict probabilities from all models
    all_preds = np.stack([m.predict_proba(chunk[features])[:,1] for m in xgb_models])
    preds = np.mean(all_preds, axis=0)
    
    # Add as column
    chunk["DELINQ_PROB"] = preds
    chunk = chunk[["LOAN_SEQUENCE_NUMBER", "MONTHLY_REPORTING_PERIOD", "DELINQUENT", "DELINQ_PROB"]]

    # Insert into DuckDB
    conn = duckdb.connect(duckdb_file)
    conn.register("chunk_df", chunk)
    conn.execute(f"INSERT INTO {table_name} SELECT * FROM chunk_df")
    conn.close()
    
    return len(chunk)