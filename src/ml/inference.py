import sys
import boto3
import joblib
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
from awsglue.utils import getResolvedOptions

def main():
    args = getResolvedOptions(sys.argv, ['BUCKET_NAME', 'TARGET_DATE'])
    bucket_name = args['BUCKET_NAME']
    target_date = args['TARGET_DATE']
    
    s3 = boto3.client('s3')
    
    # 1. Load Model (Get latest active)
    # For MVP, assume fixed path or latest listing
    # key = "artifacts/models/latest/model.pkl"
    # Mocking loading a model
    # response = s3.get_object(Bucket=bucket_name, Key=key)
    # model = joblib.load(BytesIO(response['Body'].read()))
    
    print("Loading model...")
    # Mock model for MVP execution flow
    class MockModel:
        def predict(self, X):
            return [random.uniform(10, 50) for _ in range(len(X))]
    import random
    model = MockModel()

    # 2. Load Recent Features (Last available window)
    # df_features = ...
    print("Loading inference features...")
    df_features = pd.DataFrame({
        'sector_id': ['Emergency', 'Pediatrics'],
        'rolling_avg_arrivals_4h': [12.5, 5.0],
        'lag_arrivals_24h': [10.0, 4.0]
    })
    
    X = df_features[['rolling_avg_arrivals_4h', 'lag_arrivals_24h']]
    
    # 3. Predict
    preds = model.predict(X)
    df_features['predicted_arrivals_h1'] = preds
    
    # 4. Write Predictions
    out_buffer = BytesIO()
    df_features.to_parquet(out_buffer, index=False)
    
    ts = datetime.now().strftime("%Y%m%d%H%M")
    key = f"gold/predictions/dt={target_date}/preds_{ts}.parquet"
    
    s3.put_object(Bucket=bucket_name, Key=key, Body=out_buffer.getvalue())
    print(f"Predictions saved to s3://{bucket_name}/{key}")

if __name__ == "__main__":
    main()
