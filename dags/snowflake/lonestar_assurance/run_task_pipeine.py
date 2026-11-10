import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


default_args = {
    "owner": "lonestar_assurance",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


with DAG(
    dag_id="lonestar_assurance",
    description=(
        "extract from snowflake external s3 "
        "stage to table in bronze schema"
    ),
    start_date=datetime(2026, 11, 2),
    schedule="30 3 * * *",
    catchup=False,
    render_template_as_native_obj=True,
    default_args=default_args
):

    load_to_bronze = SQLExecuteQueryOperator(
        task_id="load_from_external_stage_to_bronze",
        sql='sql/copy_stage_to_bronze.sql',
        split_statements=True,
        return_last=False,
        conn_id="snowflake_conn",
        database="{{ var.value.database }}"
    )

    run_ecs_task = EcsRunTaskOperator(
        task_id="run_ecs_task",
        task_definition="{{ var.value.task_definition }}",
        cluster="{{ var.value.cluster }}",
        overrides={},
        launch_type="FARGATE",
        do_xcom_push=False,
        network_configuration={
            "awsvpcConfiguration": "{{ var.json.network_configuration }}"  
            }
    )

load_to_bronze >> run_ecs_task
