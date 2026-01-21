import boto3
import json
import time
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Arguments from CloudFormation YAML
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SECRET_NAME',
    'RAW_BUCKET',
    'MASTER_BUCKET',
    'CURATED_BUCKET',
    'DDL_PATH',
    'REDSHIFT_IAM_ROLE',
    'WORKGROUP_NAME' 
])

# Initialize Glue/Spark Context for Delta conversion
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

secrets_manager = boto3.client('secretsmanager')
redshift_data = boto3.client('redshift-data')
s3_client = boto3.client('s3')

# to run redshift queries
def run_redshift_queries(creds, sql_test):    
    response = redshift_data.execute_statement(
                WorkgroupName=args['WORKGROUP_NAME'],
                Database=creds['dbName'],
                SecretArn=args['SECRET_NAME'],
                Sql=sql_test
                )

    query_id = response['Id']
    
    # Wait for completion
    while True:
        status_resp = redshift_data.describe_statement(Id=query_id)
        status = status_resp['Status']
        
        if status == 'FINISHED':
            print("COPY Command Completed Successfully!")
            break
        elif status in ['FAILED', 'ABORTED']:
            error = status_resp.get('Error', 'Unknown error')
            raise Exception(f"Redshift COPY failed with status {status}: {error}")
        
        print("Waiting for COPY to finish...")
        time.sleep(10)

# Getting Redshift credentials from secrets manager
response = secrets_manager.get_secret_value(SecretId=args['SECRET_NAME'])
creds = json.loads(response['SecretString'])

# list of required ddls
sql_files = [
    "dim_zones.sql",
    "dim_vendors.sql",
    "dim_ratecodes.sql",
    "trip_details.sql",
    "daily_metrics.sql",
    "vendor_performance.sql",
    "location_performance.sql"
]

for file_name in sql_files:  
    # Read SQL from S3
    s3_key = f"{args['DDL_PATH']}/{file_name}"
    obj = s3_client.get_object(Bucket=args['RAW_BUCKET'], Key=s3_key)
    ddl_text = obj['Body'].read().decode('utf-8')
    
    # To create redshift tables
    run_redshift_queries(creds, ddl_text)
    
    # to identify correct bucket
    table_name = file_name.replace(".sql", "") 
    
    if table_name.startswith("dim_"):
        source_bucket = args['MASTER_BUCKET']
        print(f"Table {table_name} identified as a Dimension. Using MASTER bucket.")
    else:
        source_bucket = args['CURATED_BUCKET']
        print(f"Table {table_name} identified as a Fact/Metric. Using CURATED bucket.")
    
    data_path = f"s3://{source_bucket}/{table_name}/"
    parquet_path = f"s3://{args['CURATED_BUCKET']}/staging_parquet/{table_name}/"

    # Delta to Parquet Conversion
    print(f"Converting {table_name} from Delta to Parquet...")
    try:
        df = spark.read.format("delta").load(data_path)
        df.write.mode("overwrite").parquet(parquet_path)
    except Exception as e:
        print(f"Skipping conversion for {table_name} (might not be delta or empty): {e}")
        continue

    copy_query = f"""
        COPY {table_name}
        FROM '{parquet_path}'
        IAM_ROLE '{args['REDSHIFT_IAM_ROLE']}'
        FORMAT AS PARQUET;
    """
    
    # To run copy command
    run_redshift_queries(creds, copy_query)

    print(f"Successfully copied {table_name}\n")