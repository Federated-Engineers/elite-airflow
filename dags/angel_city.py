from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

with DAG(
    dag_id="angel_city_dbt_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
):

    network_config = Variable.get(
        "elite_dbt_network_config",
        deserialize_json=True
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
