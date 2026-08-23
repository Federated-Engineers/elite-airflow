import datetime
from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

from business_logic.august_client_nordic_peaks.config import (
    DATA_SOURCES, S3_FOLDER_PATH, SERVICE_ACCOUNT_CREDENTIALS_PATH)
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
          task_id=f"extract_{table}",
          python_callable=load_gsheet_to_s3,
          op_kwargs={
                "connection_string": DATABASE_URL,
                "schema_name": DAG_CONFIG["schema_name"],
                "table_name": table,
                "storage_path": f"{DAG_CONFIG["bucket_path"]}/{table}/"
          }
        )

          for table in DAG_CONFIG["tables"]
    ]


    end_tasks = EmptyOperator(task_id="end")

start_tasks >> extract_sheets >> end_tasks
