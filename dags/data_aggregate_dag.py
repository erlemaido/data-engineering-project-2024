import csv
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
from airflow.models.connection import Connection

conn = Connection(
    conn_id="spark",
    conn_type="spark",
    host="spark://spark-iceberg",
    port="7077",
)
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"

default_args = {
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'run_as_user': 'sudo'
}

dag = DAG(
    'data_aggregate_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

MONGO_URI = "mongodb://mongodb:27017"
DATABASE_NAME = "default"
COLLECTION_NAME = "aggregates"


def aggregate(**kwargs):
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    with open('/opt/airflow/data/aggregate_res.csv', mode='r') as file:
        csv_reader = csv.DictReader(file)
        data = list(csv_reader)
        if data:
            for row in data:
                id = row['fiscal_year'] + "_audited_" + row['audited']
                dict = {
                    "_id": id,
                    "value": row['count(1)']
                }
                print(dict)
                collection.insert_one(dict)

    client.close()


aggregate_task = PythonOperator(
    task_id='aggregate_task',
    python_callable=aggregate,
    dag=dag
)

read_from_iceberg_task = SparkSubmitOperator(
    task_id='read_from_iceberg',
    application="/opt/airflow/dags/iceberg_import.py",
    conn_id="spark_compose",
    spark_binary="spark-submit.cmd",
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
          },
    dag=dag
)

read_from_iceberg_task >> aggregate_task
