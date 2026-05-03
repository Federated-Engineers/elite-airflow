import json
import logging

import awswrangler as wr
import pandas as pd
from airflow.sdk import Variable

from plugins.aws import get_ssm_parameter
from plugins.database import db_connection, load_db_query_results_to_s3
from plugins.date_utils import get_current_datetime
from plugins.google_sheet import get_data_from_gsheet

logger = logging.getLogger(__name__)

current_time = get_current_datetime()

config = Variable.get("liffey_luxury_config", deserialize_json=True)
sensitive_config = Variable.get("liffey_luxury_sensitive_config",
                                deserialize_json=True)

base_folder = config["s3"]["base_folder"]


def gsheet_to_s3():
    """Extract marketing data from a Google Sheet and write to S3 in Parquet
    format.

    All variables needed for this function are retrieved from Airflow Variables
    The variables include Google Sheet ID, SSM path for Google credentials,
    S3 paths.
    """

    gsheet_id = config["google_sheet"]["sheet_id"]
    google_ssm_path = sensitive_config["google_ssm_path"]

    logger.info(f"Connecting to Google Sheet with ID: {gsheet_id}")

    data = get_data_from_gsheet(gsheet_id, google_ssm_path)
    logger.info("Data extracted from Google Sheet")

    df_marketing = pd.DataFrame(data)

    if df_marketing.empty:
        raise ValueError("No data to write to S3.")

    file_name = f"{current_time}_marketing_crm.parquet"
    marketing_folder = config["s3"]["marketing_folder"]
    marketing_s3_path = (f"s3://{base_folder}/"
                         f"{marketing_folder}/{file_name}")

    wr.s3.to_parquet(df_marketing, marketing_s3_path)

    logger.info(f"Data written to S3: {marketing_s3_path}")


sql_query = "SELECT * FROM historical.liffey_luxury_order_transactions;"


def postgres_to_s3(query=sql_query):
    """Extract orders data from a PostgreSQL database and write to S3
    in Parquet format. All others variables needed for this function
    are retrieved from Airflow Variables.

    Args:
        query: The SQL query to execute against the PostgreSQL
        database.
    """

    logger.info("Starting PostgreSQL to S3 extraction.")
    db_ssm_path = sensitive_config["db_ssm_path"]
    db_cred = json.loads(get_ssm_parameter(db_ssm_path))

    con = db_connection(db_cred)
    logger.info("Database connection established.")

    query = sql_query
    file_name = f"{current_time}_orders"
    orders_folder = config["s3"]["orders_folder"]
    orders_s3_path = (f"s3://{base_folder}/"
                      f"{orders_folder}")

    logger.info("Extracting data from PostgreSQL and writing to S3.")
    load_db_query_results_to_s3(
        connection=con,
        query=query,
        base_path=orders_s3_path,
        file_name=file_name)

    logger.info(f"Data written to S3: {orders_s3_path}")
    con.close()
