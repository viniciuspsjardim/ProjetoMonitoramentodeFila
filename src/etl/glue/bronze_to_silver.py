import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_timestamp, lit

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATABASE_NAME', 'BUCKET_NAME', 'TARGET_DATE'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

db_name = args['DATABASE_NAME']
bucket_name = args['BUCKET_NAME']
target_date = args['TARGET_DATE']

# 1. Read Bronze (Encounters)
path_enc = f"s3://{bucket_name}/bronze/encounters_raw/dt={target_date}/"
try:
    dyf_enc = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [path_enc]},
        format="parquet"
    )
except Exception:
    print(f"No data found for {path_enc}")
    sys.exit(0)

df_enc = dyf_enc.toDF()

# 2. Transform Encounters
# Clean timestamps, Deduplicate
df_enc_clean = df_enc \
    .withColumn("arrival_time", to_timestamp(col("arrival_time"))) \
    .dropDuplicates(["encounter_id"]) \
    .withColumn("dt", lit(target_date))

# 3. Write to Silver (Encounters) Partitioned by dt/sector
s3_output_enc = f"s3://{bucket_name}/silver/encounters/"
df_enc_clean.write.mode("overwrite").partitionBy("dt", "sector_id").parquet(s3_output_enc)

# 4. Read Bronze (Events)
path_evt = f"s3://{bucket_name}/bronze/events_raw/dt={target_date}/"
dyf_evt = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [path_evt]},
    format="parquet"
)
df_evt = dyf_evt.toDF()

# 5. Transform Events
df_evt_clean = df_evt \
    .withColumn("event_timestamp", to_timestamp(col("event_timestamp"))) \
    .dropDuplicates(["event_id"]) \
    .withColumn("dt", lit(target_date))

# 6. Write to Silver (Events)
s3_output_evt = f"s3://{bucket_name}/silver/events/"
df_evt_clean.write.mode("overwrite").partitionBy("dt").parquet(s3_output_evt)

job.commit()
