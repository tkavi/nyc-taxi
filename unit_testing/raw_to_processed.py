import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, round, when

spark = SparkSession.builder \
        .appName("local-test") \
        .master("local[*]") \
        .getOrCreate()

# 1. Read the Raw Parquet Data
df = spark.read.parquet("./unit_testing/yellow_tripdata_2025-08.parquet")

# 2. Basic Transformation: Count rows (for logging)
row_count = df.count()
print(f"Total rows found: {row_count}")

# 2. VALIDATE & CLEAN: Remove "Impossible" Data
cleaned_df = df.filter(
    (col("fare_amount") > 0) & 
    (col("trip_distance") > 0) & 
    (col("passenger_count") > 0) &
    (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
)

# 3. TRANSFORM: Feature Engineering
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
df.write.format("delta").mode("overwrite").save("./unit_testing/processed/")

print("Local job complete!")
spark.stop()