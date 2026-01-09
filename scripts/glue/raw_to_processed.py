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

# --- Helper to load SQL from S3 ---
def load_sql(filename):
    path = f"{args['SQL_DIR']}{filename}"
    return spark.read.text(path).collect()[0][0]

# 1. Cleaning
raw_f = glueContext.create_dynamic_frame.from_catalog(
    database=args['CATALOG_DB'], 
    table_name=args['RAW_TABLE_NAME'])

raw_df = raw_f.toDF()

raw_df.createOrReplaceTempView("raw_nyc_taxi_data")
clean_df = spark.sql(load_sql("cleaning.sql"))

# 2. Trip Duration and category
clean_df.createOrReplaceTempView("clean_table")
final_df = spark.sql(load_sql("trip_duration_ctg.sql"))

# 3. Data Quality Gat
dq_rules = """
    Rules = [
        ColumnValues "fare" > 0,
        ColumnValues "trip_distance" > 0,
        ColumnValues "trip_duration_min" >= 0
    ]
"""

print("Evaluating Data Quality...")
dq_results = EvaluateDataQuality.apply(
    frame = final_df,
    ruleset = dq_rules,
    publishing_options = {
        "dataQualityTarget": f"{args['PROCESSED_PATH']}dq_results/",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True
    }
)

# --- 5. CONDITIONAL WRITE (The Governance Gate) ---
# Only write to Processed if ALL DQ rules pass.
if dq_results.all_rules_passed:
    print("DQ Passed. Writing as Delta in Processed bucket...")
    final_df.write \
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
    
    print(f"DQ FAILED. Moving {final_df.count()} records to {quarantine_path}")
    
    # 3. Write bad data as Parquet so you can analyze it with Athena later
    print("DQ FAILED. Writing to Quarantine and stopping job.")
    final_df.write \
        .format("parquet") \
        .mode("append") \
        .save(quarantine_path)
    
    job.commit() # Commit even on failure to record state  
    sys.exit("Job failed: Data Quality threshold not met. Inspect quarantine folder.")

job.commit()