import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from dags.snowflake.lonestar.copy_into_bronze import COPY_INTO_BRONZE

logger = logging.getLogger(__name__)

default_args = {
    "owner": "Federated-Engineers",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=1)
}

with DAG(
    dag_id="lonestar_dbt_pipeline",
    description="Trigger ECS Fargate task to execute Lone Star dbt workloads",
    start_date=pendulum.datetime(2026, 5, 1, tz="Africa/Lagos"),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=[
        "lonestar",
        "dbt",
        "ecs",
        "fargate",
        "snowflake"
    ]
):

    logger.info(
        "Loading network configuration from Airflow Variables"
    )

    try:
        network_config = Variable.get(
            "elite_lonestar_dbt_network",
            deserialize_json=True
        )

    except (KeyError, AirflowException) as exc:
        logger.error(
            "Airflow variable 'elite_lonestar_dbt_network' not found"
        )

        raise ValueError(
            "Airflow variable 'elite_lonestar_dbt_network' not found. "
            "Please create the variable before running this DAG."
        ) from exc

    logger.info(
        "Network configuration loaded successfully"
    )

    ingest_s3_to_snowflake = SQLExecuteQueryOperator(
        task_id="ingest_s3_to_snowflake",
        conn_id="snowflake_lonestar",
        sql=COPY_INTO_BRONZE,
        show_return_value_in_logs=True
    )

    dbt_run = EcsRunTaskOperator(
        task_id="run_dbt_lonestar",
        cluster="elite-lonestar-cluster",
        task_definition="elite_lonestar-dbt-task",
        launch_type="FARGATE",
        network_configuration={
            "awsvpcConfiguration": network_config
        },
        overrides={
            "containerOverrides": [
                {
                    "name": "elite-lonestar-dbt",
                    "command": ["./run_dbt.sh"]
                }
            ]
        },
        reattach=True,
        awslogs_group="elite_lonestar-dbt-logs",
        awslogs_region="eu-central-1",
        awslogs_stream_prefix="ecs/elite-lonestar-dbt",
        number_logs_exception=100
    )

    ingest_s3_to_snowflake >> dbt_run
