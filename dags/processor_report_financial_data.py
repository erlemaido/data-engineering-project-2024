import os
import duckdb
import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, IntegerType, NestedField
from s3_client import list_files, download_file
from iceberg_manager import upload_to_minio, create_namespace_if_not_exists, load_table, create_table_if_not_exists


BASE_DIR = '/opt/airflow/data'
ARCHIVE_DIR = os.path.join(BASE_DIR, 'archive', 'report_financial_data')
BUCKET_NAME = 'ltat-02-007-project'
PREFIX = 'report_financial_data/'
NAMESPACE = "staging"
TABLE_NAME = "report_financial_data"

def process_report_financial_data():
    conn = duckdb.connect(TABLE_NAME)
    configure_duckdb(conn)

    file_list = list_files(BUCKET_NAME, PREFIX)
    for s3_key in file_list:
        filename = os.path.basename(s3_key)
        local_filepath = os.path.join(ARCHIVE_DIR, filename)

        print(f"Downloading and processing file: {filename}")
        download_file(BUCKET_NAME, s3_key, local_filepath)

        bucket_name = "bucket"
        s3_url = f"s3://{bucket_name}/{filename}"
        upload_to_minio(local_filepath, filename)
        create_table(conn, s3_url, filename)

        print(f"Processed and archived: {filename}")

    pq_file_path = os.path.join(BASE_DIR, f"{TABLE_NAME}.parquet")
    load_table(NAMESPACE, TABLE_NAME).scan().to_pandas().to_parquet(pq_file_path)
    upload_to_minio(pq_file_path, f"{TABLE_NAME}.parquet")
    conn.close()

def configure_duckdb(conn):
    conn.sql("""
    SET s3_region='us-east-1';
    SET s3_url_style='path';
    SET s3_endpoint='minio:9000';
    SET s3_access_key_id='minioadmin';
    SET s3_secret_access_key='minioadmin';
    SET s3_use_ssl=false;
    """)

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

    create_namespace_if_not_exists(NAMESPACE)

    arrow_table = conn.sql(f"SELECT * FROM '{table_name_duck}'").arrow()
    year = int(get_year(filename))
    year_rows = pa.array([year for _ in range(arrow_table.num_rows)], type=pa.int32())
    arrow_table = arrow_table.append_column("year", [year_rows])

    create_table_if_not_exists(NAMESPACE, TABLE_NAME, schema, arrow_table)

def get_year(filename):
    filename = filename.replace("4.", "")
    filename = filename.split("_")
    return filename[0]
