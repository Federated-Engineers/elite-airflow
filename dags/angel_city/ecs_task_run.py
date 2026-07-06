import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "Federated-Engineers",
    "retries": 2,
    "retry_delay": timedelta(seconds=5)
}

with DAG(
    dag_id="angel_city_dbt_pipeline",
    description="Trigger ECS Fargate task to execute Angel City dbt workloads",
    start_date=pendulum.datetime(2026, 6, 1, tz="Africa/Lagos"),
    schedule="0 11 * * *",
    catchup=False,
    default_args=default_args,
    tags=[
        "angel-city",
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
            "elite_dbt_network_config",
            deserialize_json=True
        )

    except KeyError as exc:
        logger.error(
            "Airflow variable 'elite_dbt_network_config' not found"
        )

        raise ValueError(
            "Airflow variable 'elite_dbt_network_config' not found. "
            "Please create the variable before running this DAG."
        ) from exc

    logger.info(
        "Network configuration loaded successfully"
    )

    dbt_run = EcsRunTaskOperator(
        task_id="run_dbt",
        cluster="angel-city-health-cluster",
        task_definition="elite-dbt-task",
        launch_type="FARGATE",
        network_configuration={
            "awsvpcConfiguration": network_config
        },
        overrides={
            "containerOverrides": [
                {
                    "name": "elite-dbt",
                    "command": ["./run_dbt.sh"]
                }
            ]
        }
    )
