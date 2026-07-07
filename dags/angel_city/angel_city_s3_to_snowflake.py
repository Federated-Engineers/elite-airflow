from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': "Federated-Engineers",
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG(
    dag_id='angel_city_s3_to_snowflake_pipeline',
    default_args=default_args,
    description="Copy S3 data in S3 to Snowflake target tables",
    start_date=pendulum.datetime(2026, 6, 1, tz="Africa/Lagos"),
    schedule="0 10 * * *",
    catchup=False,
    tags=[
        "angel_city",
        "snowflake"
    ]
) as dag:

    copy_claims_to_snowflake = SQLExecuteQueryOperator(
        task_id="copy_claims",
        conn_id="snowflake_default",
        sql="sql/copy_claims.sql",
    )

    copy_diagnoses_to_snowflake = SQLExecuteQueryOperator(
        task_id="copy_diagnoses",
        conn_id="snowflake_default",
        sql="sql/copy_diagnoses.sql",
    )

    copy_encounters_to_snowflake = SQLExecuteQueryOperator(
        task_id="copy_encounters",
        conn_id="snowflake_default",
        sql="sql/copy_encounters.sql",
    )

    copy_patients_to_snowflake = SQLExecuteQueryOperator(
        task_id="copy_patients",
        conn_id="snowflake_default",
        sql="sql/copy_patients.sql",
    )

    copy_providers_to_snowflake = SQLExecuteQueryOperator(
        task_id="copy_providers",
        conn_id="snowflake_default",
        sql="sql/copy_providers.sql",
    )
