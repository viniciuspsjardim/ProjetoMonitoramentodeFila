-- Database Definition (Managed by Terraform usually, but here for ref)
-- CREATE DATABASE IF NOT EXISTS healthcare_ops_db;

-- 1. Gold Ops Metrics
CREATE EXTERNAL TABLE IF NOT EXISTS gold_ops_metrics_hourly (
    window_start TIMESTAMP,
    sector_id STRING,
    arrivals_count INT,
    avg_triage_acuity DOUBLE
)
PARTITIONED BY (dt STRING, sector_id STRING)
STORED AS PARQUET
LOCATION 's3://${bucket_name}/gold/ops_metrics_hourly/';

-- 2. Gold Predictions
CREATE EXTERNAL TABLE IF NOT EXISTS gold_predictions (
    sector_id STRING,
    predicted_arrivals_h1 DOUBLE,
    prediction_ts TIMESTAMP
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 's3://${bucket_name}/gold/predictions/';

-- 3. View: Current Status (Last 2 hours)
CREATE OR REPLACE VIEW vw_current_status AS
SELECT 
    sector_id,
    window_start,
    arrivals_count,
    avg_triage_acuity
FROM gold_ops_metrics_hourly
WHERE window_start >= current_timestamp - interval '2' hour;

-- 4. View: Forecast Next 24h
CREATE OR REPLACE VIEW vw_forecast_next_24h AS
SELECT
    sector_id,
    predicted_arrivals_h1,
    dt
FROM gold_predictions
WHERE dt = cast(current_date as string);
