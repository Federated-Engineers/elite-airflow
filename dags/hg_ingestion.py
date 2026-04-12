from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from business_logic.hg_data.hg_spreadsheet_s3 import write_sheet_to_s3

default_args = {
    'owner': 'federatedengineers',
    "retries": 2,
    "retry_delay": timedelta(seconds=5)
}

dag = DAG(
    dag_id="hg_google_sheets_to_s3",
    default_args=default_args,
    schedule="0 8 * * *",
    description="Send Lancy and Rhone Google Sheets data to S3",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["hg", "ingestion", "s3"]
)

write_lancy_s3 = PythonOperator(
    dag=dag,
    task_id="write_lancy_s3",
    python_callable=write_sheet_to_s3,
    op_kwargs={
        "sheet_id_variable": "hg_lancy_sheet_id",
        "dataset_name": "lancy",
    },
)

write_rhone_s3 = PythonOperator(
    dag=dag,
    task_id="write_rhone_s3",
    python_callable=write_sheet_to_s3,
    op_kwargs={
        "sheet_id_variable": "hg_rhone_sheet_id",
        "dataset_name": "rhone",
    },
)

[write_lancy_s3, write_rhone_s3]
