import os
import duckdb
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, IntegerType, NestedField, DateType
from client_s3 import list_files, download_file
from client_iceberg import create_namespace_if_not_exists, load_table, create_table_if_not_exists
from client_minio import upload_to_minio


BASE_DIR = '/opt/airflow/data'
ARCHIVE_DIR = os.path.join(BASE_DIR, 'archive', 'report_general_data')
BUCKET_NAME = 'ltat-02-007-project'
PREFIX = 'report_general_data/'
NAMESPACE = "staging"
TABLE_NAME = "report_general_data"

def process_report_general_data():
    conn = duckdb.connect(TABLE_NAME)
    configure_duckdb(conn)

    file_list = list_files(BUCKET_NAME, PREFIX)
    for s3_key in file_list:
        filename = os.path.basename(s3_key)
        local_filepath = os.path.join(ARCHIVE_DIR, filename)

        print(f"Downloading and processing file: {filename}")
        download_file(BUCKET_NAME, s3_key, local_filepath)

        s3_url = upload_to_minio(local_filepath, filename)
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
        "{'report_id': 'VARCHAR', 'filled_report_id': 'VARCHAR', 'reg_code': 'VARCHAR', 'legal_form': 'VARCHAR'," +
        "'status': 'VARCHAR', 'fiscal_year': 'INT', 'consolidated': 'VARCHAR', 'period_start': 'DATE'," +
        "'period_end': 'DATE', 'submission_date': 'DATE', 'audited': 'VARCHAR', 'report_category': 'VARCHAR'," +
        "'min_category': 'VARCHAR', 'audit_type': 'VARCHAR', 'audit_decision': 'VARCHAR', 'modification_main': 'VARCHAR'," +
        "'modification_other': 'VARCHAR', 'modification_cont': 'VARCHAR'})")

    schema = Schema(
        NestedField(field_id=1, name='report_id', field_type=StringType(), required=False),
        NestedField(field_id=2, name='filled_report_id', field_type=StringType(), required=False),
        NestedField(field_id=3, name='reg_code', field_type=StringType(), required=False),
        NestedField(field_id=4, name='legal_form', field_type=StringType(), required=False),
        NestedField(field_id=5, name='status', field_type=StringType(), required=False),
        NestedField(field_id=6, name='fiscal_year', field_type=IntegerType(), required=False),
        NestedField(field_id=7, name='consolidated', field_type=StringType(), required=False),
        NestedField(field_id=8, name='period_start', field_type=DateType(), required=False),
        NestedField(field_id=9, name='period_end', field_type=DateType(), required=False),
        NestedField(field_id=10, name='submission_date', field_type=DateType(), required=False),
        NestedField(field_id=11, name='audited', field_type=StringType(), required=False),
        NestedField(field_id=12, name='report_category', field_type=StringType(), required=False),
        NestedField(field_id=13, name='min_category', field_type=StringType(), required=False),
        NestedField(field_id=14, name='audit_type', field_type=StringType(), required=False),
        NestedField(field_id=15, name='audit_decision', field_type=StringType(), required=False),
        NestedField(field_id=16, name='modification_main', field_type=StringType(), required=False),
        NestedField(field_id=17, name='modification_other', field_type=StringType(), required=False),
        NestedField(field_id=18, name='modification_cont', field_type=StringType(), required=False),
    )

    create_namespace_if_not_exists(NAMESPACE)

    arrow_table = conn.sql(f"SELECT * FROM '{table_name_duck}'").arrow()

    create_table_if_not_exists(NAMESPACE, TABLE_NAME, schema, arrow_table)
