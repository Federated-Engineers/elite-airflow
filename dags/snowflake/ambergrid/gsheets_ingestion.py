from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator

from business_logic.ambergrid import config
from business_logic.ambergrid.gsheet import snapshot_worksheet_to_stage

default_args = {
    "owner": "Federated-Engineers/Ambergrid",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="ambergrid_lab_ledger_ingestion",
    description=(
        "Snapshot the AmberGrid-Lab-Ledger worksheets to the Snowflake "
        "internal stage and load them into BRONZE"
    ),
    start_date=pendulum.datetime(2026, 8, 1, tz="Africa/Lagos"),
    schedule="0 3 * * *",
    catchup=False,
    default_args=default_args,
    tags=[
        "elite",
        "ambergrid",
        "google_sheets",
        "snowflake",
        "bronze",
        "dbt_cloud"
    ]
):

    snapshot_date = "{{ data_interval_end.strftime('%Y-%m-%d') }}"

    worksheets = config.WORKSHEET_TO_BRONZE_TABLE.items()

    for worksheet, bronze_table in worksheets:

        snapshot_task_id = f"snapshot_{worksheet.lower()}"

        snapshot_to_stage = PythonOperator(
            task_id=snapshot_task_id,
            python_callable=snapshot_worksheet_to_stage,
            op_kwargs={
                "worksheet": worksheet,
                "snapshot_date": snapshot_date,
            },
        )

        copy_into_bronze = SQLExecuteQueryOperator(
            task_id=f"copy_{worksheet.lower()}_into_bronze",
            conn_id=config.SNOWFLAKE_CONN_ID,
            sql="sql/copy_gsheet_into_bronze.sql",
            params={
                "bronze_schema": config.BRONZE_SCHEMA,
                "stage_name": config.LAB_LEDGER_STAGE,
                "bronze_table": bronze_table,
                "worksheet": worksheet,
                "snapshot_task_id": snapshot_task_id,
            },
            show_return_value_in_logs=True,
        )

        snapshot_to_stage >> copy_into_bronze
