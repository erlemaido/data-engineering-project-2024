import boto3
from botocore import UNSIGNED
from botocore.config import Config

def get_s3_client(region="eu-north-1"):
    return boto3.client(
        "s3",
        region_name=region,
        config=Config(signature_version=UNSIGNED)
    )

def list_files(bucket_name, prefix, region="eu-north-1"):
    """List all CSV files in a given S3 bucket and prefix."""
    s3 = get_s3_client(region)
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    return [item['Key'] for item in response.get('Contents', []) if item['Key'].endswith('.csv')]

def download_file(bucket_name, s3_key, dest_path, region="eu-north-1"):
    """Download a file from S3 to a local path."""
    s3 = get_s3_client(region)
    with open(dest_path, 'wb') as f:
        s3.download_fileobj(bucket_name, s3_key, f)
    print(f"Downloaded: {dest_path}")
