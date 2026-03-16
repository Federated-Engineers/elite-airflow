from business_logic.alpen_mechanik.alpen_logic import alpen_elt_pipeline

import pandas as pd
import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

DAG_ID = "alpen_mechanik"


default_args = {
    'owner': 'Federated-Engineers',
    'depends_on_past': False,
    'start_date': datetime.datetime(2026, 3, 5),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=5),
    'execution_timeout': datetime.timedelta(minutes=10)
}


dag = DAG(
    DAG_ID,
    default_args=default_args,
    # schedule_interval='13 8,18 * * 1-6',
    max_active_runs=1,
    catchup=False,
    tags=[DAG_ID]
)

gsheet_to_sftp = PythonOperator(
    dag=dag,
    task_id='gsheet_to_sftp',
    python_callable=alpen_elt_pipeline
)


gsheet_to_sftp