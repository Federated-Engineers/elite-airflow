import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "kings_county_ecs_run",
    "start_date": datetime(2026, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="kings_county_ecs_run",
    default_args=default_args,
    schedule="30 0 * * *",

    description="Run elite-kings-county-dbt-task in ECS",
    catchup=False,
    tags=["kings_county", "dbt", "snowflake", "ecs"],
) as dag:

    logger.info(
        "Loading network configuration (subnets,sg,ip)"
        "from Airflow Variables"
    )

    network_config = Variable.get(
        "network_config",
        deserialize_json=True
    )

    run_task = EcsRunTaskOperator(
        task_id="run_task",
        cluster="elite-kings-county-dbt",
        task_definition="elite-kings-county-dbt-task",
        launch_type="FARGATE",

        network_configuration={
            "awsvpcConfiguration": network_config
        },

        overrides={
            "containerOverrides": [
                {
                    "name": "elite-kings-county-dbt",
                    "command": ["./run_dbt.sh"],
                        }
                    ]
        }
    )
