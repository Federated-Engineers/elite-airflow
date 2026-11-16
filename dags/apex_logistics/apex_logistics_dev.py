from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator, EmrCreateJobFlowOperator, EmrTerminateJobFlowOperator)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.standard.operators.python import PythonOperator

from business_logic.apex_logistics import config
from business_logic.apex_logistics.emr_logs import print_step_logs

default_args = {
    "owner": "Federated-Engineers/ApexLogistics",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="apex_logistics_dev",
    description=(
        "Create a dev EMR cluster, run a Spark job on it and terminate "
        "the cluster"
    ),
    start_date=pendulum.datetime(2026, 11, 1, tz="Africa/Lagos"),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    default_args=default_args,
    tags=[
        "elite",
        "apex_logistics",
        "emr",
        "spark",
        "dev",
    ]
):

    cluster_id = "{{ task_instance.xcom_pull(task_ids='create_cluster') }}"
    step_id = "{{ task_instance.xcom_pull(task_ids='add_steps')[0] }}"

    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_cluster",
        job_flow_overrides=config.JOB_FLOW_OVERRIDES,
        emr_conn_id=None,
        region_name=config.REGION,
        retries=0,
    )

    add_steps = EmrAddStepsOperator(
        task_id="add_steps",
        job_flow_id=cluster_id,
        steps=config.SMOKE_TEST_STEPS,
        region_name=config.REGION,
        retries=0,
    )

    watch_step = EmrStepSensor(
        task_id="watch_step",
        job_flow_id=cluster_id,
        step_id=step_id,
        timeout=60 * 60,
        region_name=config.REGION,
    )

    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_cluster",
        job_flow_id=cluster_id,
        trigger_rule="all_done",
        region_name=config.REGION,
    )

    step_logs = PythonOperator(
        task_id="print_step_logs",
        python_callable=print_step_logs,
        op_kwargs={
            "cluster_id": cluster_id,
            "step_id": step_id,
        },
        trigger_rule="all_done",
    )

    create_cluster >> add_steps >> watch_step >> terminate_cluster
    terminate_cluster >> step_logs
