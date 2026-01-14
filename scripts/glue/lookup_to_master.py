import sys
import datetime
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from delta.tables import DeltaTable
from fuzzywuzzy import fuzz

# Arguments from CloudFormation YAML
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 
    'RAW_BUCKET',
    'MASTER_BUCKET', 
    'SOURCE_CSV', 
    'CATALOG_DB'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Function for SCD Type 2
def upsert_scd2(master_df, target_path, pk_col):

    if DeltaTable.isDeltaTable(spark, target_path):
        dt = DeltaTable.forPath(spark, target_path)
        
        # 1. Find existing max versions to increment
        max_v = dt.toDF().filter("is_current_flag = true") \
                  .groupBy(pk_col).agg(f.max("version").alias("max_v"))
        
        updated_with_version = master_df.join(max_v, on=pk_col, how="left") \
            .withColumn("version", f.coalesce(f.col("max_v") + 1, f.lit(1))) \
            .drop("max_v")

        # 2. Close old records
        dt.alias("target").merge(
            updated_with_version.alias("source"),
            f"target.{pk_col} = source.{pk_col} AND target.is_current_flag = true"
        ).whenMatchedUpdate(set={
            "is_current_flag": f.lit(False),
            "effective_to": f.col("source.effective_from"),
            "updated_by": f.col("source.updated_by")
        }).execute()

        # 3. Write new records
        updated_with_version.write.format("delta").mode("append").save(target_path)
    else:
        # Initial Load
        master_df.withColumn("version", f.lit(1)) \
            .write.format("delta").mode("overwrite") \
            .option("mergeSchema", "true") \
            .option("delta.columnMapping.mode", "none") \
            .option("delta.minReaderVersion", "1") \
            .option("delta.minWriterVersion", "2") \
            .save(target_path)

# ==== 1. ZONES ===== #
raw_zones_df = spark.read.option("header", "true").csv(f"s3://{args['RAW_BUCKET']}/{args['SOURCE_CSV']}")

zone_window = Window.partitionBy("LocationID").orderBy(
    f.col("Borough").desc(), 
    f.col("Zone").desc()
)

# Golden records for zones
master_zones = raw_zones_df.withColumn("rn", f.row_number().over(zone_window)) \
    .filter(f.col("rn") == 1) \
    .select(
        f.col("LocationID").cast("int"),
        f.trim(f.col("Borough")).alias("borough"),
        f.trim(f.col("Zone")).alias("zone"),
        f.trim(f.col("service_zone")).alias("service_zone"),
        f.lit(args['JOB_NAME']).alias("updated_by"),
        f.lit("SYSTEM_AUTO_APPROVED").alias("approved_by"),
        f.current_timestamp().alias("effective_from"),
        f.lit(None).cast("timestamp").alias("effective_to"),
        f.lit(True).alias("is_current_flag")
    )

# Write to Master Bucket
zone_path = f"s3://{args['MASTER_BUCKET']}/dim_zones/"
upsert_scd2(master_zones, zone_path, "LocationID")    

# ==== 2. VENDORS ==== #
vendors_data = [
    (1, "Creative Mobile Technologies, LLC"),
    (2, "Curb Mobility, LLC"),
    (6, "Myle Technologies Inc"),
    (7, "Helix")
]

vendors_df = spark.createDataFrame(vendors_data, ["vendorid", "vendor_name"])

# Match Confidence
def get_fuzzy_confidence(name1, name2):
    """Returns a confidence score 0-100 based on string similarity."""
    if not name1 or not name2: 
        return 0.0
    return float(fuzz.token_set_ratio(str(name1), str(name2)))

# Registering as a UDF for Spark
fuzzy_udf = f.udf(get_fuzzy_confidence, DoubleType())

# Cross-joining to find similarities - for smaller lists
matched_vendors = vendors_df.alias("v1").crossJoin(vendors_df.alias("v2")) \
    .filter(f.col("v1.vendorid") < f.col("v2.vendorid")) \
    .withColumn("confidence", fuzzy_udf(f.col("v1.vendor_name"), f.col("v2.vendor_name")))

# Governance 
governance_df = matched_vendors.withColumn("approved_by", 
    f.when(f.col("confidence") > 95, "SYSTEM_AUTO_APPROVED")
     .when(f.col("confidence").between(80, 95), "STEWARD_REVIEW")
     .otherwise("MANUAL_RESOLUTION")
)

# sending for steward review
steward_review_df = governance_df.filter("approved_by = 'STEWARD_REVIEW'") \
    .select(
        f.col("v1.vendor_name").alias("source_vendor_name"), 
        f.col("v2.vendor_name").alias("matched_vendor_name"),
        "confidence",
        f.lit("PENDING_REVIEW").alias("status"),
        f.current_timestamp().alias("landed_at"),
        f.lit(args['JOB_NAME']).alias("detected_by_job")
    )

# notifying the steward for review
sns = boto3.client('sns')

def notify_steward(record_count):
    sns.publish(
        TopicArn='arn:aws:sns:region:account:StewardAlertTopic',
        Subject='Manual Data Review Required',
        Message=f'Job {args["JOB_NAME"]} has flagged {record_count} new vendor records for manual review. Please check the steward_review_vendor table in Athena.'
    )

# Trigger if records exist in steward_review_df
if steward_review_df.count() > 0:
    notify_steward(steward_review_df.count())

# Write to a separate folder for the Steward to see in Athena
steward_path = f"s3://{args['MASTER_BUCKET']}/steward_review/vendors/"
steward_review_df.write.format("delta").mode("append").save(steward_path)

# Golden Records for Vendors
master_vendors = spark.createDataFrame(vendors_data, ["vendorid", "vendor_name"]) \
    .select(
        f.col("vendorid"),
        f.col("vendor_name"),
        f.lit(args['JOB_NAME']).alias("updated_by"),
        f.lit("SYSTEM_AUTO_APPROVED").alias("approved_by"),
        f.current_timestamp().alias("effective_from"),
        f.lit(None).cast("timestamp").alias("effective_to"),
        f.lit(True).alias("is_current_flag")
    )

# Write to Master Bucket
vendor_path = f"s3://{args['MASTER_BUCKET']}/dim_vendors/"
upsert_scd2(master_vendors, vendor_path, "vendorid")

# ==== 3. RATE CODES ==== #
ratecodes_data = [
    (1, "Standard rate"),
    (2, "JFK"),
    (3, "Newark"),
    (4, "Nassau or Westchester"),
    (5, "Negotiated fare"),
    (6, "Group ride")
]

# Golden Records for Ratecodes
master_ratecodes = spark.createDataFrame(ratecodes_data, ["ratecodeid", "ratecode_desc"]) \
    .select(
        f.col("ratecodeid").alias("RatecodeID"),
        f.col("ratecode_desc").alias("Ratecode_desc"),
        f.lit(args['JOB_NAME']).alias("updated_by"),
        f.lit("SYSTEM_AUTO_APPROVED").alias("approved_by"),
        f.current_timestamp().alias("effective_from"),
        f.lit(None).cast("timestamp").alias("effective_to"),
        f.lit(True).alias("is_current_flag")
    )

# Write to Master Bucket
ratecode_path = f"s3://{args['MASTER_BUCKET']}/dim_ratecodes/"
upsert_scd2(master_ratecodes, ratecode_path, "RatecodeID")

# ==== ATHENA INTEGRATION ==== #
athena = boto3.client('athena')

def register_table(table_name, path):
    query = f"CREATE TABLE IF NOT EXISTS {args['CATALOG_DB']}.{table_name} \
        LOCATION '{path}' \
        TBLPROPERTIES ('table_type'='DELTA');"
    
    athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': args['CATALOG_DB']},
        ResultConfiguration={'OutputLocation': f"s3://{args['RAW_BUCKET']}/athena_queries/"}
    )

register_table("dim_zones", zone_path)
register_table("dim_vendors", vendor_path)
register_table("dim_ratecodes", ratecode_path)
register_table("steward_review_vendor", steward_path)

job.commit()