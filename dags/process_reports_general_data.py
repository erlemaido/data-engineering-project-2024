import os
import shutil

import duckdb
from pyiceberg.catalog import load_rest
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, IntegerType, NestedField, DateType

from file_processing_methods import upload_to_minio

BASE_DIR = '/opt/airflow/data'
INCOMING_DIR = os.path.join(BASE_DIR, 'incoming', 'report_general_data')
ARCHIVE_DIR = os.path.join(BASE_DIR, 'archive', 'report_general_data')
catalog = load_rest(name="rest",
                    conf={
                        "uri": "http://iceberg_rest:8181/",
                        "s3.endpoint": "http://minio:9000",
                        "s3.access-key-id": "minioadmin",
                        "s3.secret-access-key": "minioadmin",
                    },
                    )
namespace = "staging"
table_name = "report_general_data"


def process_reports_general_data():
    """
    Processes tax data files and stores them in DuckDB staging and Iceberg.
    """
    conn = duckdb.connect(table_name)
    conn.sql("INSTALL httpfs")
    conn.sql("LOAD httpfs")
    conn.sql("""
    SET s3_region='us-east-1';
    SET s3_url_style='path';
    SET s3_endpoint='minio:9000';
    SET s3_access_key_id='minioadmin' ;
    SET s3_secret_access_key='minioadmin';
    SET s3_use_ssl=false;
    """)

    for filename in os.listdir(INCOMING_DIR):
        if filename.endswith('.csv'):
            filepath = os.path.join(INCOMING_DIR, filename)
            print(f"Processing file: {filename}")
            bucket_name = "bucket"
            s3_url = f"s3://{bucket_name}/{filename}"
            upload_to_minio(filepath, filename)
            create_table(conn, s3_url, filename)

            # Move the file to the archive directory
            shutil.move(filepath, os.path.join(ARCHIVE_DIR, filename))
            print(f"Processed and archived: {filename}")

    pq_file_path = os.path.join(BASE_DIR, f"{table_name}.parquet")
    duck_file_path = os.path.join(BASE_DIR, f"{table_name}.duckdb")
    catalog.load_table(f"{namespace}.{table_name}").scan().to_pandas().to_parquet(pq_file_path)
    catalog.load_table(f"{namespace}.{table_name}").scan().to_duckdb(table_name + ".duckdb")
    conn.close()
    upload_to_minio(pq_file_path, f"{table_name}.parquet")
    upload_to_minio(duck_file_path, f"{table_name}.duckdb")


def create_table(conn, s3_url, filename):
    table_name_duck = filename.split(".")[0]
    conn.sql(
        "CREATE OR REPLACE TABLE '" + table_name_duck + "' AS SELECT * FROM read_csv('" + s3_url + "', ignore_errors=true, columns =" +
        "{'report_id': 'VARCHAR', 'filled_report_id': 'VARCHAR', 'reg_code': 'VARCHAR', 'legal_form': 'VARCHAR'," +
        "'status': 'VARCHAR', 'fiscal_year': 'INT', 'consolidated': 'VARCHAR', 'period_start': 'DATE'," +
        "'period_end': 'DATE', 'submission_date': 'DATE', 'audited': 'VARCHAR', 'report_category': 'VARCHAR'," +
        "'min_category': 'VARCHAR', 'audit_type': 'VARCHAR', 'auditor_decision': 'VARCHAR', 'modification_main': 'VARCHAR'," +
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
        NestedField(field_id=15, name='auditor_decision', field_type=StringType(), required=False),
        NestedField(field_id=16, name='modification_main', field_type=StringType(), required=False),
        NestedField(field_id=17, name='modification_other', field_type=StringType(), required=False),
        NestedField(field_id=18, name='modification_cont', field_type=StringType(), required=False),
    )

    try:
        catalog.create_namespace_if_not_exists(namespace)
    except:
        pass

    print(conn.sql(f"show tables"))
    arrow_table = conn.sql(f"SELECT * FROM '{table_name_duck}'").arrow()
    print(arrow_table)

    for i in range(arrow_table.num_columns):
        print("ARROW" + arrow_table.column_names[i])
        print("SCHEMA" + schema.column_names[i])

    table = catalog.create_table_if_not_exists(
        identifier=f"{namespace}.{table_name}",
        schema=schema,
    )

    table.append(arrow_table)


def create_duckdb_table(conn):
    bucket_name = 'bucket'
    parquet_file_name = 'report_general_data.parquet'
    s3_parquet_path = f's3://{bucket_name}/{parquet_file_name}'

    conn.sql("INSTALL iceberg")
    conn.sql("LOAD iceberg")

    "SELECT count(*) FROM iceberg_scan('s3://warehouse/staging/report_general_data', allow_moved_paths = true);"

    duckdb.sql(f"COPY my_table TO '{s3_parquet_path}' (FORMAT PARQUET)")