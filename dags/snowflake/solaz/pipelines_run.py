import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator

from business_logic.solaz.gsheets_to_snowflake import gsheets_to_snowflake
from business_logic.solaz.postgres_to_snowflake import postgres_to_snowflake

logger = logging.getLogger(__name__)

default_args = {
    "owner": "solaz_pipelines_run",
    "start_date": datetime(2026, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="solaz_pipelines_run",
    default_args=default_args,
    schedule="30 0 * * *",

    description="Pull data from S3, Gsheets and Postgres to Snowflake",
    catchup=False,
    tags=["solaz", "dbt_cloud", "snowflake", "s3", "gsheets", "postgres"],
) as dag:

    logger.info(
        "Loading network configuration (subnets,sg,ip) \
            from Airflow Variables"
    )

    network_config = Variable.get(
        "network_config",
        deserialize_json=True
    )

    refresh_bronze_external_table = SQLExecuteQueryOperator(
        task_id="refresh_bronze_external_table",
        conn_id="snowflake_default",
        sql="./refresh_query.sql")

    logger.info("External table refresh complete")

    extract_from_gsheet_and_push_to_snowflake = PythonOperator(
        task_id="extract_from_gsheet_and_push_to_snowflake",
        python_callable=gsheets_to_snowflake,
    )

    extract_from_postgres_and_push_to_snowflake = PythonOperator(
        task_id="extract_from_postgres_and_push_to_snowflake",
        python_callable=postgres_to_snowflake,
    )

    (
        refresh_bronze_external_table,
        extract_from_gsheet_and_push_to_snowflake,
        extract_from_postgres_and_push_to_snowflake,
    )
