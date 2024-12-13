import os
import duckdb
import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, IntegerType, NestedField
from s3_client import list_files, download_file
from iceberg_manager import upload_to_minio, create_namespace_if_not_exists, load_table, create_table_if_not_exists


BASE_DIR = '/opt/airflow/data'
ARCHIVE_DIR = os.path.join(BASE_DIR, 'archive', 'tax_data')
BUCKET_NAME = 'ltat-02-007-project'
PREFIX = 'tax_data/'
NAMESPACE = "staging"
TABLE_NAME = "tax_data"

def process_tax_data():
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

    create_namespace_if_not_exists(NAMESPACE)

    arrow_table = conn.sql(f"SELECT * FROM '{table_name_duck}'").arrow()
    year, quarter_str = get_year_quarter(filename)
    year = int(year)
    quarter = get_quarter(quarter_str)

    year_rows = pa.array([year for _ in range(arrow_table.num_rows)], type=pa.int32())
    quarter_rows = pa.array([quarter for _ in range(arrow_table.num_rows)], type=pa.int32())
    arrow_table = arrow_table.append_column("year", [year_rows])
    arrow_table = arrow_table.append_column("quarter", [quarter_rows])

    create_table_if_not_exists(NAMESPACE, TABLE_NAME, schema, arrow_table)

def get_year_quarter(filename: str):
    """
    Extracts year and quarter from the filename.
    """
    filename = filename.replace("_kvartal.csv", "").replace("tasutud_maksud_", "")
    parts = filename.split("_")
    if len(parts) != 2:
        raise ValueError(f"Invalid filename format: {filename}")
    return parts

def get_quarter(quarter: str):
    """
    Converts a quarter string to its numeric representation.
    """
    quarters = {"i": 1, "ii": 2, "iii": 3, "iv": 4}
    if quarter.lower() not in quarters:
        raise ValueError(f"Invalid quarter: {quarter}. Must be one of {list(quarters.keys())}.")
    return quarters[quarter.lower()]
