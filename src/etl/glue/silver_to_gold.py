import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, window, count, avg, mean, lit

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME', 'TARGET_DATE'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket_name = args['BUCKET_NAME']
target_date = args['TARGET_DATE']

# Read Silver Data
df_enc = spark.read.parquet(f"s3://{bucket_name}/silver/encounters/").filter(col("dt") == target_date)
# df_evt = spark.read.parquet(f"s3://{bucket_name}/silver/events/").filter(col("dt") == target_date) 
# Note: For MVP hourly metrics, we primarily use Encounters which have arrival_time. 
# Complex event logic (queue size) would join events, but simplifying for MVP prompt.

# Aggregate Metrics Hourly
# arrivals_count, wait_avg (if we had wait times calculated in silver, or calc here)
# Assuming wait_time is derived or available. Let's assume we calculate simple counts first.

metrics_df = df_enc.groupBy(
    "sector_id",
    window("arrival_time", "1 hour")
).agg(
    count("encounter_id").alias("arrivals_count"),
    mean("triage_level").alias("avg_triage_acuity")
).select(
    col("window.start").alias("window_start"),
    col("sector_id"),
    col("arrivals_count"),
    col("avg_triage_acuity")
).withColumn("dt", lit(target_date))

# Write to Gold
output_path = f"s3://{bucket_name}/gold/ops_metrics_hourly/"
metrics_df.write.mode("overwrite").partitionBy("dt", "sector_id").parquet(output_path)

job.commit()
