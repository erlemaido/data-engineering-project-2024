import os
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_rest
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, IntegerType, NestedField
from file_processing_methods import upload_to_minio

# Directory paths
BASE_DIR = '/opt/airflow/data'
ARCHIVE_DIR = os.path.join(BASE_DIR, 'archive', 'report_financial_data')
catalog = load_rest(name="rest",
                    conf={
                        "uri": "http://iceberg_rest:8181/",
                        "s3.endpoint": "http://minio:9000",
                        "s3.access-key-id": "minioadmin",
                        "s3.secret-access-key": "minioadmin",
                    },
                    )
namespace = "staging"
table_name = "report_financial_data"
BUCKET_NAME = "ltat-02-007-project"
PREFIX = "report_financial_data/"
REGION = "eu-north-1"

def get_anonymous_s3_client():
    """Returns an S3 client for public access with anonymous credentials."""
    return boto3.client(
        "s3",
        region_name="eu-north-1",
        config=Config(signature_version=UNSIGNED)
    )

def process_reports_financial_data():
    """
    Processes tax data files fetched from an S3 bucket and stores them in DuckDB staging and Iceberg.
    """
    # Connect to DuckDB
    conn = duckdb.connect(table_name)

    # Enable Iceberg integration (if needed)
    conn.sql("""
    SET s3_region='us-east-1';
    SET s3_url_style='path';
    SET s3_endpoint='minio:9000';
    SET s3_access_key_id='minioadmin';
    SET s3_secret_access_key='minioadmin';
    SET s3_use_ssl=false;
    """)

    # List files from S3 bucket
    s3 = get_anonymous_s3_client()
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
    file_list = [item['Key'] for item in response.get('Contents', []) if item['Key'].endswith('.csv')]

    for s3_key in file_list:
        filename = os.path.basename(s3_key)
        local_filepath = os.path.join(ARCHIVE_DIR, filename)

        print(f"Downloading and processing file: {filename}")
        download_file_from_s3(BUCKET_NAME, s3_key, local_filepath)

        bucket_name = "bucket"
        s3_url = f"s3://{bucket_name}/{filename}"
        upload_to_minio(local_filepath, filename)
        create_table(conn, s3_url, filename)

        print(f"Processed and archived: {filename}")

    pq_file_path = os.path.join(BASE_DIR, f"{table_name}.parquet")
    catalog.load_table(f"{namespace}.{table_name}").scan().to_pandas().to_parquet(pq_file_path)
    upload_to_minio(pq_file_path, f"{table_name}.parquet")
    conn.close()

def download_file_from_s3(bucket_name, s3_key, dest_path):
    """Downloads a file from S3 to a local path."""
    s3 = get_anonymous_s3_client()
    with open(dest_path, 'wb') as f:
        s3.download_fileobj(bucket_name, s3_key, f)
    print(f"Downloaded: {dest_path}")

def create_table(conn, s3_url, filename):
    table_name_duck = filename.split(".")[0]
    conn.sql(
        "CREATE OR REPLACE TABLE '" + table_name_duck + "' AS SELECT * FROM read_csv('" + s3_url + "', ignore_errors=true, columns =" +
        "{'report_id': 'VARCHAR', 'table_type': 'VARCHAR', 'element_est': 'VARCHAR', 'element_eng': 'VARCHAR'," +
        "'element_value': 'INT'})")

    schema = Schema(
        NestedField(field_id=1, name='report_id', field_type=StringType(), required=False),
        NestedField(field_id=2, name='table_type', field_type=StringType(), required=False),
        NestedField(field_id=3, name='element_est', field_type=StringType(), required=False),
        NestedField(field_id=4, name='element_eng', field_type=StringType(), required=False),
        NestedField(field_id=5, name='element_value', field_type=IntegerType(), required=False),
        NestedField(field_id=6, name='year', field_type=IntegerType(), required=False),
    )

    try:
        catalog.create_namespace_if_not_exists(namespace)
    except:
        pass

    print(conn.sql(f"show tables"))
    arrow_table = conn.sql(f"SELECT * FROM '{table_name_duck}'").arrow()
    year = int(get_year(filename))
    year_rows = pa.array([year for _ in range(arrow_table.num_rows)], type=pa.int32())
    arrow_table = arrow_table.append_column("year", [year_rows])

    table = catalog.create_table_if_not_exists(
        identifier=f"{namespace}.{table_name}",
        schema=schema,
    )

    table.append(arrow_table)

def get_year(filename):
    filename = filename.replace("4.", "")
    filename = filename.split("_")
    return filename[0]
