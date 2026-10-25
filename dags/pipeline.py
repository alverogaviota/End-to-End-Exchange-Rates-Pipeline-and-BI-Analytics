from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'exchange_rates_elt',
    default_args=default_args,
    description='Batch ELT pipeline for exchange rates',
    start_date=datetime(2026, 1, 1),
    schedule='30 8 * * *',
    catchup=False,
) as dag:

    ingest_task = BashOperator(
        task_id='run_ingestion',
        bash_command='python "/mnt/c/Users/AlveroGaviota/Desktop/End-to-End Exchange Rates Pipeline & BI Analytics/src/01_ingestion.py"',
        env={
            "DATABRICKS_SERVER_HOSTNAME": "{{conn.databricks_default.host}}",
            "DATABRICKS_HTTP_PATH": "{{conn.databricks_default.schema}}",
            "DATABRICKS_TOKEN": "{{conn.databricks_default.password}}",
        },
        append_env=True
    )

    transform_task = BashOperator(
        task_id='run_transformation',
        bash_command='python "/mnt/c/Users/AlveroGaviota/Desktop/End-to-End Exchange Rates Pipeline & BI Analytics/src/02_transform.py"',
        env={
            "DATABRICKS_SERVER_HOSTNAME": "{{conn.databricks_default.host}}",
            "DATABRICKS_HTTP_PATH": "{{conn.databricks_default.schema}}",
            "DATABRICKS_TOKEN": "{{conn.databricks_default.password}}",
        },
        append_env=True
    )

    aggregate_task = BashOperator(
        task_id='run_aggregation',
        bash_command='python "/mnt/c/Users/AlveroGaviota/Desktop/End-to-End Exchange Rates Pipeline & BI Analytics/src/03_aggregate.py"',
        env={
            "DATABRICKS_SERVER_HOSTNAME": "{{conn.databricks_default.host}}",
            "DATABRICKS_HTTP_PATH": "{{conn.databricks_default.schema}}",
            "DATABRICKS_TOKEN": "{{conn.databricks_default.password}}",
        },
        append_env=True
    )

    ingest_task >> transform_task >> aggregate_task