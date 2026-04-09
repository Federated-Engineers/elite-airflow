import logging

import awswrangler as wr
import pandas as pd
from sqlalchemy import create_engine

from plugins.date_utils import get_current_datetime
from plugins.google_sheet import get_data_from_gsheet

logger = logging.getLogger(__name__)

SSM_PATH = "/production/google-service-account/credentials"

BUCKET_NAME = "federated-engineers-staging-elite-data-lake"
FOLDER = "liffey_luxury"

current_time = get_current_datetime()
MARKETING_S3_PATH = (f"s3://{BUCKET_NAME}/{FOLDER}/raw/marketing/"
                     f"{current_time}_marketing_crm.parquet")
ORDERS_S3_PATH = (f"s3://{BUCKET_NAME}/{FOLDER}/raw/orders/"
                  f"{current_time}_orders.parquet")


def gsheet_to_s3(gsheet_id: str):
    """Extract data from a Google Sheet and write to S3 in Parquet format.

    Args:
        gsheet_id: The ID of the Google Sheet to extract data from.
    """

    data = get_data_from_gsheet(gsheet_id, SSM_PATH)
    logger.info(f"Data extracted from Google Sheet with ID: {gsheet_id}")

    df_marketing = pd.DataFrame(data)

    if df_marketing.empty:
        logger.warning("No data found in the Google Sheet.")
        raise ValueError("No data to write to S3.")

    wr.s3.to_parquet(df_marketing, MARKETING_S3_PATH)
    logger.info(f"Data written to S3: {MARKETING_S3_PATH}")


# PostgreSQL extraction
query = """
    SELECT *
    FROM historical.liffey_luxury_order_transactions;
"""


def postgres_to_s3(url: str, query: str = query):
    """Extract data from a PostgreSQL database and write to S3 in
    Parquet format.

    Args:
        url: The environment variable name containing the database
        connection URL.
        query: The SQL query to execute against the PostgreSQL
        database.
    """

    logger.info("Starting PostgreSQL to S3 extraction.")

    if not url:
        logger.error("Database environment variable is not set.")
        raise ValueError("Database environment variable is missing")

    logger.info("Connecting to PostgreSQL database.")
    engine = create_engine(url)

    df_orders = pd.read_sql(query, engine)
    logger.info(f"Data extracted from PostgreSQL: {df_orders.shape[0]} rows")

    if df_orders.empty:
        logger.warning("No data found in the PostgreSQL query.")
        raise ValueError("No data to write to S3.")

    wr.s3.to_parquet(df_orders, ORDERS_S3_PATH)
    logger.info(f"Data written to S3: {ORDERS_S3_PATH}")
