import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python import PythonOperator

from client_mongo import insert_aggregates_to_mongo

os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    'aggregate_audit_data_dag',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
) as dag:

    read_from_iceberg = SparkSubmitOperator(
        task_id='read_from_iceberg',
        application="/opt/airflow/dags/iceberg_import.py",
        conn_id="spark_compose",
        conf={'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
              'spark.sql.defaultCatalog': 'rest',
              'spark.sql.catalog.rest': 'org.apache.iceberg.spark.SparkCatalog',
              'spark.sql.catalog.rest.catalog-impl': 'org.apache.iceberg.rest.RESTCatalog',
              'spark.sql.catalog.rest.uri': 'http://iceberg_rest:8181/',
              'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
              'spark.sql.catalog.rest.warehouse': 's3a://warehouse',
              'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
              'spark.sql.catalog.rest.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
              'spark.sql.catalog.rest.s3.endpoint': 'http://minio:9000',
              'spark.executorEnv.AWS_REGION': 'us-east-1',
              'spark.jars.packages': 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.80.0,software.amazon.awssdk:bundle:2.24.8,software.amazon.awssdk:url-connection-client:2.24.8'
          }
    )

    aggregate_data = PythonOperator(
        task_id='aggregate_data',
        python_callable=insert_aggregates_to_mongo
    )

    read_from_iceberg >> aggregate_data
