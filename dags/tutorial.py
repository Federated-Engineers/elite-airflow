from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from business_logic.tutorial import combine_region_sales, get_region_sales

default_args = {
    "owner": "Federated-Engineers",
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
    "execution_timeout": timedelta(minutes=10)
}

with DAG(
    dag_id="tutorial_demo",
    description="Get sales for 4 regions in parallel, then combine them",
    start_date=pendulum.datetime(2026, 1, 1, tz="Africa/Lagos"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=[
        "tutorial",
        "demo"
    ]
):

    get_north_sales = PythonOperator(
        task_id="get_north_sales",
        python_callable=get_region_sales,
        op_kwargs={"region": "north"}
    )

    get_south_sales = PythonOperator(
        task_id="get_south_sales",
        python_callable=get_region_sales,
        op_kwargs={"region": "south"}
    )

    get_east_sales = PythonOperator(
        task_id="get_east_sales",
        python_callable=get_region_sales,
        op_kwargs={"region": "east"}
    )

    get_west_sales = PythonOperator(
        task_id="get_west_sales",
        python_callable=get_region_sales,
        op_kwargs={"region": "west"}
    )

    combine_sales = PythonOperator(
        task_id="combine_sales",
        python_callable=combine_region_sales
    )

    [
        get_north_sales,
        get_south_sales,
        get_east_sales,
        get_west_sales
    ] >> combine_sales
