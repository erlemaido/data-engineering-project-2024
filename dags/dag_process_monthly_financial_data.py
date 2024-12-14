from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from processor_report_financial_data import process_report_financial_data
from processor_report_general_data import process_report_general_data
from processor_tax_data import process_tax_data

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the DAG
with DAG(
    'process_monthly_financial_data_dag',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
) as dag:

    # Define processing tasks
    process_reports_general_data = PythonOperator(
        task_id='process_reports_general_data',
        python_callable=process_report_general_data,
    )

    process_reports_financial_data = PythonOperator(
        task_id='process_reports_financial_data',
        python_callable=process_report_financial_data,
    )

    process_tax_data = PythonOperator(
        task_id='process_tax_data',
        python_callable=process_tax_data,
    )

    # dbt run task
    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='cd /opt/airflow/dbt/ && dbt run --project-dir /opt/airflow/dbt/',
    )

    process_reports_general_data >> process_reports_financial_data >> process_tax_data >> run_dbt