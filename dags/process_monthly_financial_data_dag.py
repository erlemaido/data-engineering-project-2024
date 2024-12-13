from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from process_reports_financial_data import process_reports_financial_data
from process_reports_general_data import process_reports_general_data
from process_tax_data import process_tax_data

# Define the DAG
with DAG(
    'process_monthly_financial_data_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@monthly',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
) as dag:

    # Define processing tasks
    process_reports_general_data = PythonOperator(
        task_id='process_reports_general_data',
        python_callable=process_reports_general_data,
    )

    process_reports_financial_data = PythonOperator(
        task_id='process_reports_financial_data',
        python_callable=process_reports_financial_data,
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

