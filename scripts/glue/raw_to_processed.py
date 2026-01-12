import sys
import datetime

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

from pyspark.context import SparkContext
from pyspark.sql.functions import col, unix_timestamp, round, when
from pyspark.sql import SparkSession

# These arguments are passed from the CloudFormation YAML
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'RAW_TABLE_NAME', 
    'CATALOG_DB', 
    'PROCESSED_PATH', 
    'SQL_DIR'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# To load SQL from S3
def load_sql(filename):
    path = f"{args['SQL_DIR']}{filename}"
    return spark.read.text(path).collect()[0][0]

# 1. Standardization
raw_f = glueContext.create_dynamic_frame.from_catalog(
    database=args['CATALOG_DB'], 
    table_name=args['RAW_TABLE_NAME'])

raw_df = raw_f.toDF()

raw_df.createOrReplaceTempView("raw_nyc_taxi_data")
standardized_df = spark.sql(load_sql("standardization.sql").strip())

# 2. Trips Validation
standardized_df.createOrReplaceTempView("standardized_nyc_taxi_data")
valid_trips_df = spark.sql(load_sql("valid_trips.sql").strip())

# 3. Fares Validation
valid_trips_df.createOrReplaceTempView("validated_nyc_taxi_data")
valid_fares_df = spark.sql(load_sql("valid_fares.sql").strip())

# Data Quality Gat
dq_rules = """
    Rules = [
        ColumnValues "vendor_id" in [1, 2, 6, 7],
        ColumnValues "payment_type_id" <= 6,
        IsComplete "pickup_location_id",
        IsComplete "dropoff_location_id",
        ColumnValues "total_amount" >= 0,
        ColumnValues "fare_amount" >= 0
    ]
"""

print("Evaluating Data Quality...")
dq_results = EvaluateDataQuality.apply(
    frame = valid_fares_df,
    ruleset = dq_rules,
    publishing_options = {
        "dataQualityTarget": f"{args['PROCESSED_PATH']}dq_results/",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True
    }
)

# CONDITIONAL WRITE (The Governance Gate)
# Only write to Processed if ALL DQ rules pass.
if dq_results.all_rules_passed:
    print("DQ Passed. Writing as Delta in Processed bucket...")
    valid_fares_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(args['PROCESSED_PATH'])
else:
    # 1. Generate a timestamped path for the quarantine folder
    run_date = datetime.datetime.now().strftime("year=%Y/month=%m/day=%d/run=%H%M")
    
    # 2. Extract the Raw Bucket name from your PROCESSD_PATH
    raw_bucket_base = args['PROCESSED_PATH'].replace("processed", "raw").split('/')[0:3]
    raw_bucket_url = "/".join(raw_bucket_base)
    quarantine_path = f"{raw_bucket_base}/quarantine/{run_date}/"
    
    print(f"DQ FAILED. Moving batch of {valid_fares_df.count()} records to {quarantine_path}")
    
    # 3. Write bad data as Parquet so you can analyze it with Athena later
    print("DQ FAILED. Writing to Quarantine and stopping job.")
    valid_fares_df.write \
        .format("parquet") \
        .mode("append") \
        .save(quarantine_path)
    
    job.commit() # Commit even on failure to record state  
    sys.exit("Job failed: Data Quality threshold not met. Batch quarantined. Inspect quarantine folder.")

job.commit()