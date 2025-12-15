import sys
import boto3
import json
from awsglue.utils import getResolvedOptions

# Simple DQ Check for MVP (can be expanded to Great Expectations)
def run_checks(bucket_name, target_date):
    s3 = boto3.client('s3')
    
    # Check 1: Files exist in Silver
    prefix = f"silver/encounters/dt={target_date}/"
    resp = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if 'Contents' not in resp:
        return {"status": "FAILED", "reason": "No Silver Data found"}
    
    # Check 2: Row Counts (mock logic for S3 select or simple Glue read)
    # real production would use Spark or Athena query
    
    return {"status": "PASSED", "checks": ["existence_verified"]}

def main():
    args = getResolvedOptions(sys.argv, ['BUCKET_NAME', 'TARGET_DATE'])
    bucket_name = args['BUCKET_NAME']
    target_date = args['TARGET_DATE']
    
    result = run_checks(bucket_name, target_date)
    
    # Write Report
    report_key = f"gold/data_quality_runs/dt={target_date}/report.json"
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket_name, Key=report_key, Body=json.dumps(result))
    
    if result["status"] == "FAILED":
        print("Data Quality Failed")
        sys.exit(1)
    else:
        print("Data Quality Passed")

if __name__ == "__main__":
    main()
