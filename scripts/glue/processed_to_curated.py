import sys
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as f

# Arguments from CloudFormation YAML
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 
    'CATALOG_DB', 
    'PROCESSED_BUCKET', 
    'MASTER_BUCKET',
    'CURATED_BUCKET',   
    'RAW_BUCKET'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# To overcome the legacy issues of Athena with latest Delta
spark.conf.set("spark.databricks.delta.properties.defaults.columnMapping.mode", "none")
spark.conf.set("spark.databricks.delta.properties.defaults.minReaderVersion", "1")
spark.conf.set("spark.databricks.delta.properties.defaults.minWriterVersion", "2")

# Load processed (fact) and master (dimensional) data
processed_df = spark.read.format("delta").load(f"s3://{args['PROCESSED_BUCKET']}/processed_files/").cache()

zones_df = spark.read.format("delta") \
    .load(f"s3://{args['MASTER_BUCKET']}/dim_zones/") \
    .filter(f.col("is_current_flag") == True) \
    .cache()

vendors_df= spark.read.format("delta") \
    .load(f"s3://{args['MASTER_BUCKET']}/dim_vendors/") \
    .filter(f.col("is_current_flag") == True) \
    .select(f.col("vendorid").alias("vendor_id"), "vendor_name") \
    .cache()

ratecodes_df = spark.read.format("delta") \
    .load(f"s3://{args['MASTER_BUCKET']}/dim_ratecodes/") \
    .filter(f.col("is_current_flag") == True) \
    .select(f.col("ratecodeid").alias("ratecode_id"), "ratecode_desc") \
    .cache()

# Business Transformations
# 1. Join for Pickup & Dropoff Locations
pickup_dropoff_df = processed_df \
        .join(zones_df.select(
                f.col("locationid").alias("pulocation_id"), 
                f.col("zone").alias("pickup_zone"), 
                f.col("borough").alias("pickup_borough")),
            f.col("pulocationid") == f.col("pulocation_id"),
            "left"
        ).join(zones_df.select(
                f.col("locationid").alias("dolocation_id"), 
                f.col("zone").alias("dropoff_zone"),
                f.col("borough").alias("dropoff_borough")), 
            f.col("dolocationid") == f.col("dolocation_id"), 
            "left"
        ).join(vendors_df, f.col("vendorid") == f.col("vendor_id"), "left") \
        .join(ratecodes_df, f.col("ratecodeid") == f.col("ratecode_id"), "left") \
        .drop("pulocation_id", "dolocation_id","vendor_id","ratecode_id")

pickup_dropoff_df.cache()

trip_details_df = pickup_dropoff_df.select(
    "vendorid",
    "vendor_name",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "pulocationid",
    "pickup_zone",
    "pickup_borough",
    "dolocationid",
    "dropoff_zone",
    "dropoff_borough",
    "passenger_count",
    "trip_distance",
    "ratecodeid",
    "ratecode_desc",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "cbd_congestion_fee"
)

trip_details_path = f"s3://{args['CURATED_BUCKET']}/trip_details/"

trip_details_df.write.format("delta").mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(trip_details_path)


# 2. Daily revenue and tip performance
daily_metrics_df = trip_details_df.groupBy(
    f.window("tpep_pickup_datetime", "1 day").alias("pickup_day"),
    "vendor_name"
).agg(
    f.count("*").alias("total_trips"),
    f.round(f.sum("total_amount")/1000000, 2).alias("total_revenue"),
    f.round(f.avg("tip_amount"), 2).alias("avg_tip"),
    f.round(f.avg(
        f.col("tip_amount") / f.when(f.col("total_amount") - f.col("tip_amount") <= 0, None)
        .otherwise(f.col("total_amount") - f.col("tip_amount")) * 100
    ), 2).alias("avg_tip_percentage"),
    f.max("trip_distance").alias("longest_trip_in_miles")
).select(
    f.col("pickup_day.start").alias("trip_date"),
    "vendor_name",
    "total_trips",
    f.col("total_revenue").alias("daily_revenue_in_M"),
    f.col("avg_tip").alias("avg_tip"),
    "avg_tip_percentage",
    "longest_trip_in_miles"
)

daily_metrics_path = f"s3://{args['CURATED_BUCKET']}/daily_metrics/"

daily_metrics_df.write.format("delta").mode("overwrite") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .save(daily_metrics_path)


# 3: Location Performance
print("Calculating Location Performance...")
location_perf_df = trip_details_df.groupBy("pickup_zone", "pickup_borough") \
    .agg(
        f.count("*").alias("pickup_count"),
        f.round(f.avg("fare_amount"),2).alias("avg_fare_by_zone")
    ).orderBy(f.desc("pickup_count"))

location_perf_path = f"s3://{args['CURATED_BUCKET']}/location_performance/"

location_perf_df.write.format("delta").mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(location_perf_path)


# 4. Vendor Performance
vendor_perf_df = trip_details_df.groupBy("vendor_name").agg(
    f.count("*").alias("total_trips"),
    f.round(f.sum("total_amount")/1000000,2).alias("revenue_in_M")
)

vendor_perf_path = f"s3://{args['CURATED_BUCKET']}/vendor_performance/"

vendor_perf_df.write.format("delta").mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(vendor_perf_path)

# ==== Athena Integration ==== #
athena = boto3.client('athena')

def register_athena(table_name, path):
    query = f"""CREATE EXTERNAL TABLE IF NOT EXISTS `{args['CATALOG_DB']}`.`{table_name}`
            LOCATION '{path}' \
            TBLPROPERTIES ('table_type'='DELTA');"""
    
    athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': args['CATALOG_DB']},
        ResultConfiguration={'OutputLocation': f"s3://{args['RAW_BUCKET']}/athena_results/"}
    )

register_athena("trip_details", trip_details_path)
register_athena("daily_metrics", daily_metrics_path)
register_athena("location_performance", location_perf_path)
register_athena("vendor_performance", vendor_perf_path)

pickup_dropoff_df.unpersist()
zones_df.unpersist()
vendors_df.unpersist()
ratecodes_df.unpersist()

job.commit()