from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

from business_logic.august_client_nordic_peaks.config import (
    DATA_SOURCES,
    S3_FOLDER_PATH,
    SERVICE_ACCOUNT_CREDENTIALS_PATH,
)
from business_logic.august_client_nordic_peaks.module import load_gsheet_to_s3

default_args = {
    "owner": "client_nordic_peak",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "catchup": False
}

with DAG(
    dag_id="client_nordic_peaks",
    start_date=datetime(2026, 8, 20),
    schedule="0 23 * * *",
    default_args=default_args
):

    start_tasks = EmptyOperator(task_id="start")

    extract_sheets = [
        PythonOperator(
            task_id=f"extract_{sheet_name}",
            python_callable=load_gsheet_to_s3,
            op_kwargs={
                "googlesheet_id": sheet_id,
                "ssm_path": SERVICE_ACCOUNT_CREDENTIALS_PATH,
                "folder_path": S3_FOLDER_PATH,
                "file_name": sheet_name
            }
        )

        for sheet_name, sheet_id in DATA_SOURCES.items()
    ]

    end_tasks = EmptyOperator(task_id="end")

start_tasks >> extract_sheets >> end_tasks
