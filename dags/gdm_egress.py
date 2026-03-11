from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from datetime import timedelta

from business_logic.gdm.gdm_script import extract_portugal_data

default_args = {
    "owner": "gdm",
    "start_date": datetime(2026, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="gdm_egress",
    default_args=default_args,
    schedule="0 8 * * *",
    catchup=False
) as dag:

    latest_portugal_file = PythonOperator(
        task_id="latest_portugal_file",
        python_callable=extract_portugal_data
    )