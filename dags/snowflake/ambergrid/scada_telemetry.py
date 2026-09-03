from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from business_logic.ambergrid import config

default_args = {
    "owner": "Federated-Engineers/Ambergrid",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=15),
}

with DAG(
    dag_id="ambergrid_scada_telemetry",
    description=(
        "Refresh the partition metadata of the SCADA telemetry external "
        "table so newly landed and rewritten S3 files become visible"
    ),
    start_date=pendulum.datetime(2026, 8, 1, tz="Africa/Lagos"),
    schedule="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=[
        "elite",
        "ambergrid",
        "scada",
        "snowflake",
        "bronze",
        "dbt_cloud"
    ]
):

    refresh_external_table = SQLExecuteQueryOperator(
        task_id="refresh_scada_telemetry",
        conn_id=config.SNOWFLAKE_CONN_ID,
        sql="sql/refresh_scada_external_table.sql",
        params={
            "bronze_schema": config.BRONZE_SCHEMA,
            "external_table": config.SCADA_EXTERNAL_TABLE,
        },
        show_return_value_in_logs=True,
    )

    refresh_external_table
