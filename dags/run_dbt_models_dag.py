from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

with DAG(
    'run_dbt_models_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt/ && dbt run --project-dir /opt/airflow/dbt/',
    )
