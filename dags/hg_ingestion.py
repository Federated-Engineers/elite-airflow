from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from business_logic.hg_data.hg_spreadsheet_s3 import (write_lancy_to_s3,
                                                      write_rhone_to_s3)

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
)

write_lancy_s3 = PythonOperator(
    dag=dag,
    task_id="write_lancy_s3",
    python_callable=write_lancy_to_s3,
)

write_rhone_s3 = PythonOperator(
    dag=dag,
    task_id="write_rhone_s3",
    python_callable=write_rhone_to_s3,
)

write_lancy_s3 >> write_rhone_s3
