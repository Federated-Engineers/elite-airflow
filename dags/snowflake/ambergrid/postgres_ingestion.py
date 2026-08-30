from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator

from business_logic.ambergrid import config
from business_logic.ambergrid.postgres import snapshot_table_to_stage

default_args = {
    "owner": "Federated-Engineers/Ambergrid",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="ambergrid_postgres_ingestion",
    description=(
        "Snapshot the AmberGrid operational Postgres tables to the Snowflake "
        "internal stage and merge them into BRONZE"
    ),
    start_date=pendulum.datetime(2026, 8, 1, tz="Africa/Lagos"),
    schedule="30 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=[
        "elite",
        "ambergrid",
        "postgres",
        "snowflake",
        "bronze",
        "dbt_cloud"
    ]
):

    snapshot_date = "{{ data_interval_end.strftime('%Y-%m-%d') }}"

    source_tables = config.POSTGRES_TABLE_TO_BRONZE_TABLE.items()

    for source_table, (bronze_table, row_key) in source_tables:

        snapshot_task_id = f"snapshot_{source_table}"

        snapshot_to_stage = PythonOperator(
            task_id=snapshot_task_id,
            python_callable=snapshot_table_to_stage,
            op_kwargs={
                "source_table": source_table,
                "snapshot_date": snapshot_date,
            },
        )

        merge_into_bronze = SQLExecuteQueryOperator(
            task_id=f"merge_{source_table}_into_bronze",
            conn_id=config.SNOWFLAKE_CONN_ID,
            sql="sql/merge_postgres_into_bronze.sql",
            params={
                "bronze_schema": config.BRONZE_SCHEMA,
                "stage_name": config.PG_STAGE,
                "bronze_table": bronze_table,
                "row_key": row_key,
                "source_table": source_table,
                "snapshot_task_id": snapshot_task_id,
            },
            show_return_value_in_logs=True,
        )

        snapshot_to_stage >> merge_into_bronze
