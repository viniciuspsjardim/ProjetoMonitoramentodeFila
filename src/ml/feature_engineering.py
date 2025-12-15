import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lag, avg, lit
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME', 'TARGET_DATE'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket_name = args['BUCKET_NAME']
target_date = args['TARGET_DATE']

# Read Gold Ops Metrics
df_metrics = spark.read.parquet(f"s3://{bucket_name}/gold/ops_metrics_hourly/")

# Feature Engineering Logic
# 1. Rolling Windows (Last 4h)
# 2. Lags (1h, 24h)

window_spec = Window.partitionBy("sector_id").orderBy("window_start")

df_features = df_metrics.withColumn(
    "rolling_avg_arrivals_4h",
    avg("arrivals_count").over(window_spec.rowsBetween(-3, 0))
).withColumn(
    "lag_arrivals_24h",
    lag("arrivals_count", 24).over(window_spec)
).na.drop() # Drop rows with nulls (early periods)

# Add Target columns (Shifted Future Values)
# e.g., Target +1h = Lead 1h
# For inference, we don't need targets, just features. 
# This script creates the Feature Store view.
# If creating TRAINING data, we would add leads.
# Prompt 09 says: "Targets: wait_time_h1, h4, h24". So we assume this is creating the Training Dataset.

from pyspark.sql.functions import lead

df_features_with_targets = df_features.withColumn(
    "target_arrivals_h1", lead("arrivals_count", 1).over(window_spec)
).withColumn(
    "target_arrivals_h4", lead("arrivals_count", 4).over(window_spec)
).withColumn(
    "target_arrivals_h24", lead("arrivals_count", 24).over(window_spec)
)

# Write to Gold Features
output_path = f"s3://{bucket_name}/gold/features/"
df_features_with_targets.write.mode("overwrite").partitionBy("dt", "sector_id").parquet(output_path)

job.commit()
