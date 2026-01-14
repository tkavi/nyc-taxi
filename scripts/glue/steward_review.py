import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from delta.tables import DeltaTable
from pyspark.sql import functions as f

# Arguments from CloudFormation YAML
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 
    'MASTER_BUCKET', 
    'CATALOG_DB'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)

# required paths
steward_path = f"s3://{args['MASTER_BUCKET']}/steward_review/vendors/"
vendor_path = f"s3://{args['MASTER_BUCKET']}/dim_vendors/"

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

# 1. Load approved records - We assume the steward used Athena to set status = 'APPROVED'
approved_df = spark.read.format("delta").load(steward_path) \
    .filter("status = 'APPROVED'")

if approved_df.count() > 0:
    # 2. Write to dim_vendors
    promoted_vendors = approved_df.select(
        f.col("vendorid"),
        f.col("matched_vendor_name").alias("vendor_name"), # Use the corrected name
        f.lit(args['JOB_NAME']).alias("updated_by"),
        f.lit("STEWARD_APPROVED").alias("approved_by"),
        f.current_timestamp().alias("effective_from"),
        f.lit(None).cast("timestamp").alias("effective_to"),
        f.lit(True).alias("is_current_flag")
    )

    # 3. To update the main table
    upsert_scd2(promoted_vendors, vendor_path, "vendorid")

    # 4. Mark as PROCESSED in the steward table so they don't run again
    steward_dt = DeltaTable.forPath(spark, steward_path)
    steward_dt.update(
        condition = "status = 'APPROVED'",
        set = { "status": f.lit("PROCESSED") }
    )

print("Steward review complete.")