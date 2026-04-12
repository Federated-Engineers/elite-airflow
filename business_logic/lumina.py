import json
import logging

import awswrangler as wr
import pandas as pd
from airflow.models import Variable

from plugins.aws import get_ssm_parameter
from plugins.psycopg2_connection import db_connection

logging.basicConfig(level=logging.INFO)

config = Variable.get("lumina_bricks_config", deserialize_json=True)
PROJECT_DIR = config["project_dir"]
BUCKET_NAME = config["bucket_name"]
SCHEMA_NAME = config["schema_name"]


BUCKET_PATH = f"{BUCKET_NAME}/{PROJECT_DIR}"

db_cred = json.loads(get_ssm_parameter('/supabase/database/credentials'))

s3_base_path = f"s3://{BUCKET_PATH}"

TABLES_TO_MIGRATE = [
        "historical_transactions",
        "property_metadata",
        "renovation_ledgers",
        "neighborhood_demographics",
        "zoning_permits",
    ]


def migrate_table(
        connection: str,
        table_name: str,
        base_path: str,
        schema_name: str = "historical",
        ):
    """
    Parameters:
    - schema_name: schema where the table exists
    - table_name: table to migrate
    - base_path: S3 destination path
    """
    query = f"SELECT * FROM {schema_name}.{table_name}"
    file_path = f"{base_path}/{table_name}.parquet"

    try:
        logging.info(f"reading table: {table_name}...")

        df = pd.read_sql_query(query, connection)

        logging.info(f"Uploading to S3: {file_path}")
        wr.s3.to_parquet(
            df=df,
            path=file_path,
            dataset=False,
        )
    except Exception as e:
        logging.error(f"Error migrating {table_name}: {e}")
        raise e
    finally:
        connection.close()


migrate_table(
    connection=db_connection(db_cred),
    table_name="historical_transactions",
    base_path=s3_base_path
)
