from datetime import datetime, timedelta
from airflow import DAG
import duckdb
import pandas as pd
from airflow.operators.python import PythonOperator
from pyiceberg.catalog import load_rest
from minio import Minio
from minio.error import S3Error

default_args = {
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'data_load_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)


def upload_to_minio(**kwargs):
    minio_endpoint = "minio:9000"  # Replace with your MinIO server's URL
    access_key = "minioadmin"
    secret_key = "minioadmin"
    bucket_name = "practice-bucket"
    file_path = "/opt/airflow/data/maksud_kvartal_raw.parquet"  # Path to your local CSV file
    object_name = "maksud_kvartal_raw.parquet"  # The name you want to assign to the file in the bucket

    client = Minio(
        minio_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )

    try:
        found = client.bucket_exists(bucket_name)
        if not found:
            client.make_bucket(bucket_name)
        else:
            print(f"Bucket '{bucket_name}' already exists.")

        client.fput_object(bucket_name, object_name, file_path)
        print(f"'{file_path}' is successfully uploaded as '{object_name}' to bucket '{bucket_name}'.")

    except S3Error as e:
        print("S3Error: ", e)
    except Exception as e:
        print("Error: ", e)


upload_to_minio_task = PythonOperator(
    task_id='upload_to_minio',
    python_callable=upload_to_minio,
    dag=dag
)


def extract_data(**kwargs):
    df = pd.read_csv('/opt/airflow/data/tasutud_maksud_2024_i_kvartal.csv', on_bad_lines='skip', sep=';')
    df.to_parquet('/opt/airflow/data/maksud_kvartal_raw.parquet')


extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)


def create_duckdb_table(**kwargs):
    conn = duckdb.connect()  # use duckdb.co
    conn.install_extension("httpfs")
    conn.load_extension("httpfs")
    conn.sql("""
    SET s3_region='us-east-1';
    SET s3_url_style='path';
    SET s3_endpoint='minio:9000';
    SET s3_access_key_id='minioadmin' ;
    SET s3_secret_access_key='minioadmin';
    SET s3_use_ssl=false;
    """)

    bucket_name = "practice-bucket"
    file_name = "maksud_kvartal_raw.parquet"
    s3_url = f"s3://{bucket_name}/{file_name}"

    # Read data from the MinIO bucket using DuckDB into a pyarrow dataframe
    conn.sql(f"SELECT * FROM read_parquet('{s3_url}')")

    # Create a table named "tmp" based on the JSON data.
    conn.sql(f"CREATE TABLE tmp AS SELECT * FROM read_parquet('{s3_url}')")

    catalog = load_rest(name="rest",
                        conf={
                            "uri": "http://iceberg_rest:8181/",
                            "s3.endpoint": "http://minio:9000",
                            "s3.access-key-id": "minioadmin",
                            "s3.secret-access-key": "minioadmin",
                        },
                        )
    namespace = "default"
    table_name = "tmp_table5"
    try:
        catalog.create_namespace(namespace)
    except:
        pass

    arrow_table = conn.sql("SELECT * FROM tmp").arrow()
    schema = arrow_table.schema

    table = catalog.create_table(
        identifier=f"{namespace}.{table_name}",
        schema=schema,
    )

    table.append(arrow_table)


duckdb_task = PythonOperator(
    task_id='create_duckdb_table',
    python_callable=create_duckdb_table,
    dag=dag
)

extract_task >> upload_to_minio_task >> duckdb_task
