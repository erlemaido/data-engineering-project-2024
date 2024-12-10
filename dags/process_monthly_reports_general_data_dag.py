from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from process_reports_general_data import process_reports_general_data

with DAG(
    'process_monthly_reports_general_data_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@monthly',
    catchup=False,
) as dag:
    process_reports_general_data = PythonOperator(
        task_id='process_reports_general_data',
        python_callable=process_reports_general_data,
    )

