import datetime
from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from business_logic.balti_logix.balt import run_compaction_task

default_args = {
    "start_date": datetime.datetime(2026, 3, 18),
    "retries": 2,
    "retry_delay": timedelta(seconds=30)
}


with DAG(
    dag_id="baltix_dag",
    default_args=default_args,
    schedule="0 1 * * *",
    catchup=True,
    description="Daily compaction for BaltiLogix JSON files",
    tags=["compaction", "s3", "balti-logix"]
) as dag:

    compaction = PythonOperator(
        task_id="compact_baltilogix",
        python_callable=run_compaction_task,
    )
