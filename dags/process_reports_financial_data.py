import os
import shutil

import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_rest
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, IntegerType, NestedField

from file_processing_methods import upload_to_minio

# Directory paths
BASE_DIR = '/opt/airflow/data'
INCOMING_DIR = os.path.join(BASE_DIR, 'incoming', 'report_financial_data')
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


def process_reports_financial_data():
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

    # Process each incoming file
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
    catalog.load_table(f"{namespace}.{table_name}").scan().to_pandas().to_parquet(pq_file_path)
    upload_to_minio(pq_file_path, f"{table_name}.parquet")
    conn.close()


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
