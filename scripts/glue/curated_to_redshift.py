import boto3
import json
import time
import sys
from awsglue.utils import getResolvedOptions

# Arguments from CloudFormation YAML
args = getResolvedOptions(sys.argv, [
    'SECRET_NAME'
    'CURATED_BUCKET',
    'REDSHIFT_IAM_ROLE',
    'WORKGROUP_NAME' 
])

secrets_manager = boto3.client('secretsmanager')
redshift_data = boto3.client('redshift-data')

# For S3 copy from curated to Redshift 
def run_redshift_copy(creds, target_table, iam_role, workgroup_name): 
    s3_path = f"s3://{args['CURATED_BUCKET']}/{target_table}"

    copy_query = f"""
        COPY {target_table}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        FORMAT AS PARQUET;
    """
    
    print(f"Starting COPY to {target_table}...")
    
    response = redshift_data.execute_statement(
        WorkgroupName=workgroup_name,
        Database=creds['dbName'],
        DbUser=creds['username'],
        Sql=copy_query
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

# List of tables to copy
TARGET_TABLES = [
    'trip_details', 
    'daily_metrics',
    'location_performance', 
    'vendor_performance' 
]

# Getting Redshift credentials 
response = secrets_manager.get_secret_value(args['SECRET_NAME'])
creds = json.loads(response['SecretString'])

for table in TARGET_TABLES:
    run_redshift_copy(
        creds, 
        table, 
        args['REDSHIFT_IAM_ROLE'],
        args['WORKGROUP_NAME']
    )