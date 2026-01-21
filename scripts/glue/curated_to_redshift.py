import boto3
import json
import time
import sys
from awsglue.utils import getResolvedOptions

# Arguments from CloudFormation YAML
args = getResolvedOptions(sys.argv, [
    'SECRET_NAME',
    'RAW_BUCKET',
    'CURATED_BUCKET',
    'DDL_PATH',
    'REDSHIFT_IAM_ROLE',
    'WORKGROUP_NAME' 
])

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
        time.sleep(4)

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
    print(f"Reading {file_name} from S3...")
    s3_key = f"{args['DDL_PATH']}/{file_name}"
    obj = s3_client.get_object(Bucket=args['RAW_BUCKET'], Key=s3_key)
    ddl_text = obj['Body'].read().decode('utf-8')
    
    print(f"Creating table using {file_name}...")
    run_redshift_queries(creds, ddl_text)
    
    table_name = file_name.replace(".sql", "") 
    data_path = f"s3://{args['CURATED_BUCKET']}/{table_name}/"

    # Run COPY command
    copy_query = f"""
        COPY {table_name}
        FROM '{data_path}'
        IAM_ROLE '{args['REDSHIFT_IAM_ROLE']}'
        FORMAT AS PARQUET;
    """

    run_redshift_queries(creds, copy_query)
    print(f"Successfully copied {table_name}\n")