import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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

# 3. Write to Processed Bucket as Snappy Parquet
df.write.mode("overwrite").parquet(args['PROCESSED_PATH'])

job.commit()