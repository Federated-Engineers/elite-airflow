import datetime
from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from business_logic.scardinavas.gsheet_to_s3 import gsheet_to_s3_dataset

default_args = {
    'start_date': datetime.datetime(2026, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=2),
    'catchup': False
}

dag = DAG(
    dag_id='scardinavas_dag',
    default_args=default_args,
    schedule='0 8 * * *',
    description='A DAG to send google sheets data to S3',
)

write_orders_s3 = PythonOperator(
        dag=dag,
        python_callable=gsheet_to_s3_dataset,
        task_id='write_orders_s3',
        op_kwargs={
            "gsheet_id": "1UOEAk2VqruxVnMW-BHHPSgLp3lKDNBIr9BcyaDCMIy0",
            "ssm_path": "/production/google-service-account/credentials",
            "bucket": "federated-engineers-production-elite-scardinavas",
            "path_dir": "orders_data",
            "database": "elite-production-scardinavas",
            "table_name": "orders",
            "date_column": "order_date"
        }
    )

write_shipments_s3 = PythonOperator(
        dag=dag,
        python_callable=gsheet_to_s3_dataset,
        task_id='write_shipments_s3',
        op_kwargs={
            "gsheet_id": "1HVENFa9QYKnLU0i-qxybSU0BI1o0hOML-zrlWqmlmaE",
            "ssm_path": "/production/google-service-account/credentials",
            "bucket": "federated-engineers-production-elite-scardinavas",
            "path_dir": "shipments_data",
            "database": "elite-production-scardinavas",
            "table_name": "shipments",
            "date_column": "shipment_date"
        }
    )

write_payments_s3 = PythonOperator(
        dag=dag,
        python_callable=gsheet_to_s3_dataset,
        task_id='write_payments_s3',
        op_kwargs={
            "gsheet_id": "1_hVaaoYq1oFL0njuBVkynleK5FTw6nwAaOE4NcmLC78",
            "ssm_path": "/production/google-service-account/credentials",
            "bucket": "federated-engineers-production-elite-scardinavas",
            "path_dir": "payments_data",
            "database": "elite-production-scardinavas",
            "table_name": "payments",
            "date_column": "payment_date"

        }
    )

write_orders_s3
write_shipments_s3
write_payments_s3
