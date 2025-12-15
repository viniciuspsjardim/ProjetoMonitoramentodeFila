import os
import sys
import shutil
import pandas as pd
import numpy as np
from datetime import datetime
import logging

# Ensure src is in path to import generator
sys.path.append(os.path.abspath("src"))
from data_gen import generate_synthetic_data as gen

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = "./data_local"
LAYERS = ["bronze", "silver", "gold"]

def setup_dirs():
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)
    for layer in LAYERS:
        os.makedirs(os.path.join(DATA_DIR, layer), exist_ok=True)
    
    # Subfolders
    os.makedirs(os.path.join(DATA_DIR, "bronze/encounters"), exist_ok=True)
    os.makedirs(os.path.join(DATA_DIR, "bronze/events"), exist_ok=True)
    os.makedirs(os.path.join(DATA_DIR, "silver/encounters"), exist_ok=True)
    os.makedirs(os.path.join(DATA_DIR, "silver/events"), exist_ok=True)
    os.makedirs(os.path.join(DATA_DIR, "gold/ops_metrics_hourly"), exist_ok=True)
    os.makedirs(os.path.join(DATA_DIR, "gold/predictions"), exist_ok=True)
    os.makedirs(os.path.join(DATA_DIR, "artifacts/models"), exist_ok=True)

def run_bronze_ingestion(target_date):
    logger.info(">>> [1] Bronze Ingestion (Synthetic Data)")
    df_enc, df_evts = gen.generate_encounters(target_date, gen.SECTORS)
    
    # Save as Parquet
    df_enc.to_parquet(f"{DATA_DIR}/bronze/encounters/data.parquet", index=False)
    df_evts.to_parquet(f"{DATA_DIR}/bronze/events/data.parquet", index=False)
    logger.info(f"Generated {len(df_enc)} encounters and {len(df_evts)} events.")

def run_silver_transformation():
    logger.info(">>> [2] Bronze -> Silver Transformation (Cleaning)")
    
    # Read Bronze
    df_enc = pd.read_parquet(f"{DATA_DIR}/bronze/encounters/data.parquet")
    df_evts = pd.read_parquet(f"{DATA_DIR}/bronze/events/data.parquet")
    
    # Transform Encounters
    # Simulating Spark Logic: to_timestamp, dropDuplicates
    df_enc['arrival_time'] = pd.to_datetime(df_enc['arrival_time'])
    df_enc = df_enc.drop_duplicates(subset=['encounter_id'])
    
    # Transform Events
    df_evts['event_timestamp'] = pd.to_datetime(df_evts['event_timestamp'])
    df_evts = df_evts.drop_duplicates(subset=['event_id'])
    
    # Write Silver
    df_enc.to_parquet(f"{DATA_DIR}/silver/encounters/data.parquet", index=False)
    df_evts.to_parquet(f"{DATA_DIR}/silver/events/data.parquet", index=False)
    logger.info("Silver Cleaned and Saved.")

def run_gold_aggregation(target_date):
    logger.info(">>> [3] Silver -> Gold Transformation (Hourly Metrics)")
    
    df_enc = pd.read_parquet(f"{DATA_DIR}/silver/encounters/data.parquet")
    
    # Aggregation Logic
    # Set index to arrival_time for resampling
    df_enc.set_index('arrival_time', inplace=True)
    
    # Convert 'triage_level' to numeric, coercing errors just in case
    df_enc['triage_level'] = pd.to_numeric(df_enc['triage_level'], errors='coerce')

    # Group by Sector and Resample Hourly
    metrics = []
    for sector, group in df_enc.groupby('sector_id'):
        hourly = group.resample('1h').agg({
            'encounter_id': 'count',
            'triage_level': 'mean'
        }).rename(columns={'encounter_id': 'arrivals_count', 'triage_level': 'avg_triage_acuity'})
        
        hourly['sector_id'] = sector
        metrics.append(hourly)
    
    df_metrics = pd.concat(metrics).reset_index()
    df_metrics['dt'] = target_date
    
    # Write Gold
    df_metrics.to_parquet(f"{DATA_DIR}/gold/ops_metrics_hourly/data.parquet", index=False)
    logger.info(f"Calculated {len(df_metrics)} hourly metric records.")
    return df_metrics

def run_ml_pipeline(target_date):
    logger.info(">>> [4] ML Pipeline (Train & Predict)")
    
    # Load Gold Metrics
    df = pd.read_parquet(f"{DATA_DIR}/gold/ops_metrics_hourly/data.parquet")
    
    if df.empty:
        logger.warning("No data for ML.")
        return

    # Basic Feature Engineering for Demo
    # Rolling average (simulated with Pandas rolling)
    df.sort_values(['sector_id', 'arrival_time'], inplace=True)
    df['rolling_avg_arrivals_4h'] = df.groupby('sector_id')['arrivals_count'].transform(lambda x: x.rolling(4, min_periods=1).mean())
    df['lag_arrivals_24h'] = df.groupby('sector_id')['arrivals_count'].shift(24).fillna(0)
    
    # Mock Training
    from sklearn.ensemble import GradientBoostingRegressor
    X = df[['rolling_avg_arrivals_4h', 'lag_arrivals_24h']]
    y = df['arrivals_count'] # Auto-regressive for simplicity
    
    model = GradientBoostingRegressor()
    model.fit(X, y)
    
    # Save Model
    import joblib
    joblib.dump(model, f"{DATA_DIR}/artifacts/models/model.pkl")
    logger.info("Model Trained and Saved locally.")
    
    # Inference (Next 1h prediction)
    # Using last known data
    latest_data = df.groupby('sector_id').tail(1).copy()
    X_latest = latest_data[['rolling_avg_arrivals_4h', 'lag_arrivals_24h']]
    preds = model.predict(X_latest)
    
    latest_data['predicted_arrivals_h1'] = preds
    latest_data['prediction_ts'] = pd.Timestamp.now()
    
    output = latest_data[['sector_id', 'prediction_ts', 'predicted_arrivals_h1']]
    output.to_parquet(f"{DATA_DIR}/gold/predictions/preds.parquet", index=False)
    
    logger.info("Inference Generated:")
    print(output)

if __name__ == "__main__":
    target_date = datetime.now().strftime("%Y-%m-%d")
    
    setup_dirs()
    run_bronze_ingestion(target_date)
    run_silver_transformation()
    metrics = run_gold_aggregation(target_date)
    run_ml_pipeline(target_date)
    
    logger.info("=== Local Pipeline Execution Completed ===")
    logger.info(f"Check {DATA_DIR} for outputs.")
