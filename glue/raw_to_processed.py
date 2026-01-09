import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, unix_timestamp, round, when

# These arguments are passed from the CloudFormation YAML
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'RAW_PATH', 'PROCESSED_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(f"Reading data from: {args['RAW_PATH']}")

# 1. Read the Raw Parquet Data
df = spark.read.parquet(args['RAW_PATH'])

# 2. Basic Transformation: Count rows (for logging)
row_count = df.count()
print(f"Total rows found: {row_count}")

# 2. VALIDATE & CLEAN: Remove "Impossible" Data
# - Fare must be positive
# - Trip distance must be greater than 0
# - Passenger count must be at least 1 (usually)
# - Dropoff must be after Pickup
cleaned_df = df.filter(
    (col("fare_amount") > 0) & 
    (col("trip_distance") > 0) & 
    (col("passenger_count") > 0) &
    (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
)

# 3. TRANSFORM: Feature Engineering
# Calculate Trip Duration in minutes
cleaned_df = cleaned_df.withColumn(
    "trip_duration_minutes",
    round((unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60, 2)
)

# 4. HANDLE NULLS: Fill missing flags with "N"
cleaned_df = cleaned_df.fillna({"store_and_fwd_flag": "N"})

# Log cleaning results
final_count = cleaned_df.count()
print(f"Cleaning complete. Removed {row_count - final_count} bad records.")

# 3. Write to Processed Bucket as delta
# df.write.mode("overwrite").parquet(args['PROCESSED_PATH'])
df.write.format("delta").mode("overwrite").save(args['PROCESSED_PATH'])

# To perform an Upsert (Merge)
# from delta.tables import DeltaTable

# deltaTable = DeltaTable.forPath(spark, args['PROCESSED_PATH'])
# deltaTable.alias("oldData").merge(
#     newData.alias("newData"),
#     "oldData.id = newData.id"
# ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

job.commit()