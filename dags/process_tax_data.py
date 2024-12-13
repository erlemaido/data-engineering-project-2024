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
ARCHIVE_DIR = os.path.join(BASE_DIR, 'archive', 'tax_data')
catalog = load_rest(name="rest",
                    conf={
                        "uri": "http://iceberg_rest:8181/",
                        "s3.endpoint": "http://minio:9000",
                        "s3.access-key-id": "minioadmin",
                        "s3.secret-access-key": "minioadmin",
                    },
                    )
namespace = "staging"
table_name = "tax_data"
BUCKET_NAME = "ltat-02-007-project"
PREFIX = "tax_data/"
REGION = "eu-north-1"

def get_anonymous_s3_client():
    """Returns an S3 client for public access with anonymous credentials."""
    return boto3.client(
        "s3",
        region_name="eu-north-1",
        config=Config(signature_version=UNSIGNED)
    )

def process_tax_data():
    """
    Processes tax data files and stores them in DuckDB staging and Iceberg.
    """
    # Connect to DuckDB
    conn = duckdb.connect(table_name)

    # Enable Iceberg integration (if needed)
    conn.sql("""
    SET s3_region='us-east-1';
    SET s3_url_style='path';
    SET s3_endpoint='minio:9000';
    SET s3_access_key_id='minioadmin' ;
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
        "{'reg_code': 'VARCHAR', 'entity_name': 'VARCHAR', 'entity_type': 'VARCHAR', 'sales_tax_reg': 'VARCHAR'," +
        "'EMTAK_field': 'VARCHAR', 'county': 'INT', 'state_taxes': 'INT', 'employee_taxes': 'INT'," +
        "'revenue': 'INT', 'employees': 'INT'})")

    schema = Schema(
        NestedField(field_id=1, name='reg_code', field_type=StringType(), required=False),
        NestedField(field_id=2, name='entity_name', field_type=StringType(), required=False),
        NestedField(field_id=3, name='entity_type', field_type=StringType(), required=False),
        NestedField(field_id=4, name='sales_tax_reg', field_type=StringType(), required=False),
        NestedField(field_id=5, name='EMTAK_field', field_type=StringType(), required=False),
        NestedField(field_id=6, name='county', field_type=IntegerType(), required=False),
        NestedField(field_id=7, name='state_taxes', field_type=IntegerType(), required=False),
        NestedField(field_id=8, name='employee_taxes', field_type=IntegerType(), required=False),
        NestedField(field_id=9, name='revenue', field_type=IntegerType(), required=False),
        NestedField(field_id=10, name='employees', field_type=IntegerType(), required=False),
        NestedField(field_id=11, name='year', field_type=IntegerType(), required=False),
        NestedField(field_id=12, name='quarter', field_type=IntegerType(), required=False)
    )

    try:
        catalog.create_namespace_if_not_exists(namespace)
    except:
        pass

    print(conn.sql(f"show tables"))
    arrow_table = conn.sql(f"SELECT * FROM '{table_name_duck}'").arrow()
    year = int(get_year(get_year_quarter(filename)[0]))
    quarter = int(get_quarter(get_year_quarter(filename)[1]))
    year_rows = pa.array([year for _ in range(arrow_table.num_rows)], type=pa.int32())
    quarter_rows = pa.array([quarter for _ in range(arrow_table.num_rows)], type=pa.int32())
    arrow_table_year = arrow_table.append_column("year", [year_rows])
    arrow_table_quarter = arrow_table_year.append_column("quarter", [quarter_rows])

    table = catalog.create_table_if_not_exists(
        identifier=f"{namespace}.{table_name}",
        schema=schema,
    )

    table.append(arrow_table_quarter)



def get_year_quarter(filename: str):
    filename = filename.replace("_kvartal.csv", "")
    filename = filename.replace("tasutud_maksud_", "")
    return filename.split("_")


def get_year(filename: str):
    return int(get_year_quarter(filename)[0])


def get_quarter(quarter: str):
    if quarter == "i":
        return 1
    elif quarter == "ii":
        return 2
    elif quarter == "iii":
        return 3
    elif quarter == "iv":
        return 4
