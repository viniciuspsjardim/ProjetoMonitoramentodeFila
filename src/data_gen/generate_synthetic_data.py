import argparse
import logging
import random
import uuid
import boto3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from io import BytesIO

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
SECTORS = ['Emergency', 'Pediatrics', 'Orthopedics', 'General']
TRIAGE_LEVELS = [1, 2, 3, 4, 5]  # 1=Critical, 5=Non-urgent
TRIAGE_DIST = [0.05, 0.15, 0.40, 0.30, 0.10]  # Probability distribution
HOURLY_PEAKS = {
    8: 1.2, 9: 1.3, 10: 1.5, 11: 1.4, 
    18: 1.3, 19: 1.5, 20: 1.4, 21: 1.2
} # Multipliers for arrival rate

def get_arrival_rate(hour):
    base_rate = 20 # arrivals per hour base
    multiplier = HOURLY_PEAKS.get(hour, 0.8) # Off-peak default
    return int(base_rate * multiplier)

def generate_encounters(date_str, sectors):
    logger.info(f"Generating data for {date_str}...")
    start_of_day = datetime.strptime(date_str, "%Y-%m-%d")
    
    encounters = []
    events = []
    
    for hour in range(24):
        current_hour_dt = start_of_day + timedelta(hours=hour)
        
        for sector in sectors:
            num_arrivals = get_arrival_rate(hour)
            # Add randomness to arrivals
            actual_arrivals = int(np.random.poisson(num_arrivals))
            
            for _ in range(actual_arrivals):
                encounter_id = str(uuid.uuid4())
                patient_id = f"PAT-{random.randint(10000, 99999)}"
                
                # Arrival Time (random within hour)
                arrival_min = random.randint(0, 59)
                arrival_time = current_hour_dt + timedelta(minutes=arrival_min)
                
                triage_level = np.random.choice(TRIAGE_LEVELS, p=TRIAGE_DIST)
                
                encounters.append({
                    "encounter_id": encounter_id,
                    "arrival_time": arrival_time,
                    "patient_id": patient_id,
                    "sector_id": sector,
                    "triage_level": triage_level,
                    "dt": date_str
                })
                
                # Generate Events (Long tail logic simulated by exponential/gamma)
                # Triage Wait
                wait_triage =  int(np.random.gamma(shape=2, scale=10)) # mins
                triage_start = arrival_time + timedelta(minutes=wait_triage)
                triage_duration = random.randint(2, 10)
                triage_end = triage_start + timedelta(minutes=triage_duration)
                
                events.extend([
                    {"event_id": str(uuid.uuid4()), "encounter_id": encounter_id, "event_type": "triage_start", "event_timestamp": triage_start, "dt": date_str},
                    {"event_id": str(uuid.uuid4()), "encounter_id": encounter_id, "event_type": "triage_end", "event_timestamp": triage_end, "dt": date_str}
                ])
                
                # Service Wait
                wait_service = int(np.random.exponential(scale=30)) # mins
                care_start = triage_end + timedelta(minutes=wait_service)
                
                events.append(
                    {"event_id": str(uuid.uuid4()), "encounter_id": encounter_id, "event_type": "care_start", "event_timestamp": care_start, "dt": date_str}
                )

    return pd.DataFrame(encounters), pd.DataFrame(events)

def upload_to_s3(df, bucket, folder, date_str):
    if df.empty:
        logger.warning(f"No data to upload for {folder}")
        return

    # Partition by dt
    s3_path = f"bronze/{folder}/dt={date_str}/{uuid.uuid4()}.parquet"
    
    # In-memory buffer
    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)
    
    s3 = boto3.client('s3')
    try:
        s3.put_object(Bucket=bucket, Key=s3_path, Body=out_buffer.getvalue())
        logger.info(f"Uploaded {len(df)} rows to s3://{bucket}/{s3_path}")
    except Exception as e:
        logger.error(f"Failed to upload to S3: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=datetime.now().strftime("%Y-%m-%d"), help="YYYY-MM-DD")
    parser.add_argument("--bucket", type=str, required=True, help="Target S3 Bucket")
    parser.add_argument("--local", action="store_true", help="Save locally instead of S3")
    
    args = parser.parse_args()
    
    df_enc, df_evts = generate_encounters(args.date, SECTORS)
    
    if args.local:
        df_enc.to_parquet(f"encounters_{args.date}.parquet")
        df_evts.to_parquet(f"events_{args.date}.parquet")
        logger.info("Saved locally.")
    else:
        upload_to_s3(df_enc, args.bucket, "encounters_raw", args.date)
        upload_to_s3(df_evts, args.bucket, "events_raw", args.date)
