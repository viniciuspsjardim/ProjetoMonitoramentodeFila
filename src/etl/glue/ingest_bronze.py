import sys
import boto3
import logging
from datetime import datetime, timedelta
from awsglue.utils import getResolvedOptions

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME', 'TARGET_DATE'])
    bucket_name = args['BUCKET_NAME']
    target_date = args['TARGET_DATE']
    
    logger.info(f"Starting Bronze Ingestion for date: {target_date}")
    
    # In a real scenario, this would check a watermark/API.
    # For MVP, we invoke the synthetic generator logic (imported or subprocess)
    # Here we assume the synthetic data script modules are available or we execute it directly.
    # To keep it simple for MVP Glue Python Shell, we might inline the generation or call the script if packaged.
    
    # Simulating import of the generation logic (assuming the file is properly packaged in a .whl or .zip for Glue)
    # For this file, we will basically replicate the "run" logic or assume subprocess if the script is present.
    
    # Ideally, we trigger the generation. 
    # NOTE: In the MVP architecture, this script IS the entry point that calls the generator.
    
    try:
        # Import dynamically if in the same zip, or just subprocess
        import generate_synthetic_data as gen
        
        # We need to adapt the generator to be callable or run it via subprocess
        # Assuming we can call the functions:
        df_enc, df_evts = gen.generate_encounters(target_date, gen.SECTORS)
        
        gen.upload_to_s3(df_enc, bucket_name, "encounters_raw", target_date)
        gen.upload_to_s3(df_evts, bucket_name, "events_raw", target_date)
        
        logger.info("Bronze Ingestion Completed Successfully.")
        
    except ImportError:
        logger.error("Could not import generate_synthetic_data. Ensure it is packaged with the job.")
        # Fallback to subprocess if needed, but import is better
        raise

if __name__ == "__main__":
    main()
