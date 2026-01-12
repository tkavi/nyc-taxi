import sys
import datetime

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
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
    sql_content = sc.wholeTextFiles(path).collect()[0][1]
    # return spark.read.text(path).collect()[0][0]
    return sql_content.strip()

# 1. Standardization
raw_dynf = glueContext.create_dynamic_frame.from_catalog(
    database=args['CATALOG_DB'], 
    table_name=args['RAW_TABLE_NAME'])

# Glue DynamicFrame to Spark DF
raw_df = raw_dynf.toDF()

raw_df.createOrReplaceTempView("raw_nyc_taxi_data")
query = load_sql("standardization.sql")
print(f"Executing Query: {query}")
# standardized_df = spark.sql(load_sql("standardization.sql").strip())
standardized_df = spark.sql(query)

# 2. Trips Validation
standardized_df.createOrReplaceTempView("standardized_nyc_taxi_data")
valid_trips_df = spark.sql(load_sql("valid_trips.sql"))

# 3. Fares Validation
valid_trips_df.createOrReplaceTempView("validated_nyc_taxi_data")
valid_fares_df = spark.sql(load_sql("valid_fares.sql"))

# to view sample data
for row in valid_fares_df.take(10):
    print(row.asDict())

# Converting Spark Df back to Glue DynamicFrame
valid_fares_dynf = DynamicFrame.fromDF(valid_fares_df, glueContext, "valid_fares_dynf")

# Data Quality Gate
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
    frame = valid_fares_dynf,
    ruleset = dq_rules,
    publishing_options = {
        "dataQualityTarget": f"{args['PROCESSED_PATH']}dq_results/",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True
    }
)

# Converting DynamicFrame to Spark Df for easy filtering
dq_df = dq_results.toDF()

# The 'Outcome' column will contain 'Passed', 'Failed', or 'Error'
failed_count = dq_df.filter(col("Outcome") == "Failed").count()

# Conditional Write (The Governance Gate)
# Only writing to Processed if ALL DQ rules pass.
if failed_count == 0:
    print("DQ Passed. Writing as Delta in Processed bucket...")
    valid_fares_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(args['PROCESSED_PATH'])
else:
    print("Printing Failed Rules:")
    dq_df.filter(col("Outcome") == "Failed").select("EvaluatedMetrics", "FailureReason", "Outcome", "Rule").show()

    # Generating a timestamped path for the quarantine folder
    run_date = datetime.datetime.now().strftime("year=%Y/month=%m/day=%d/run=%H%M")
    
    # Extracting the Raw Bucket name from PROCESSED_PATH
    raw_bucket_base = args['PROCESSED_PATH'].replace("processed", "raw").split('/')[0:3]
    raw_bucket_url = "/".join(raw_bucket_base)
    quarantine_path = f"{raw_bucket_base}/quarantine/{run_date}/"
    
    print(f"DQ FAILED. Moving batch of {valid_fares_df.count()} records to {quarantine_path}")
    
    # Writing bad data as Parquet to analyze with Athena later
    print("DQ FAILED. Writing to Quarantine and stopping job.")
    valid_fares_df.write \
        .format("parquet") \
        .mode("append") \
        .save(quarantine_path)
    
    job.commit() # Commit even on failure to record state  
    sys.exit("Job failed: Data Quality threshold not met. Batch quarantined. Inspect quarantine folder.")

job.commit()