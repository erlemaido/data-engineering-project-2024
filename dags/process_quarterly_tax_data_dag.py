from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from process_tax_data import process_tax_data

with DAG(
    'process_quarterly_tax_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 0 11 1,4,7,10 *',
    catchup=False,
) as dag:
    process_tax_data = PythonOperator(
        task_id='process_tax_data',
        python_callable=process_tax_data,
    )