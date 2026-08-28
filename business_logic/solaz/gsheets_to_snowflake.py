import logging

import pandas as pd
from airflow.sdk import Variable

from plugins.google_sheet import get_data_from_gsheet
from plugins.snowflake_helper import (establish_snowflake_connection,
                                      write_dataframe_to_snowflake)

logger = logging.getLogger(__name__)

config = Variable.get("solaz_config", deserialize_json=True)
sensitive_config = Variable.get("solaz_sensitive_config",
                                deserialize_json=True)


def get_last_transaction_timestamp(connection, database, schema, table):
    """ Get last/max transaction timestamp from orders table in Snowflake."""

    cursor = connection.cursor()

    try:
        cursor.execute(
            f"""
            SELECT MAX(TRANSACTION_TIMESTAMP)
            FROM {database}.{schema}.{table};
            """
        )

        snowflake_max_timestamp = cursor.fetchone()[0]
        logger.info(
            "Last transaction timestamp in Snowflake: %s",
            snowflake_max_timestamp,
        )

        return snowflake_max_timestamp

    finally:
        cursor.close()


def get_new_orders(orders_df, snowflake_max_timestamp):
    """Check for Google Sheets data created since the last Snowflake load."""

    # Handles first full load
    if snowflake_max_timestamp is None:
        logger.info(f"Full load to Snowflake {len(orders_df)}")
        return orders_df

    # Implements incremental load
    orders_df["TRANSACTION_TIMESTAMP"] = pd.to_datetime(
        orders_df["TRANSACTION_TIMESTAMP"]
    )

    new_orders = orders_df[
        orders_df["TRANSACTION_TIMESTAMP"] > snowflake_max_timestamp
    ].copy()
    logger.info(f"{len(new_orders)} new data found")

    return new_orders


def gsheets_to_snowflake():
    """Extract daily orders data from a Google Sheet and write to Snowflake.

    All variables needed for this function are retrieved from Airflow Variables
    The variables include Google Sheet ID, SSM path for Google credentials
    """

    gsheet_id = config["google_sheet"]["sheet_id"]
    sheet_name = config["google_sheet"]["sheet_name"]
    google_ssm_path = sensitive_config["google_ssm_path"]

    logger.info(f"Connecting to Google Sheet with ID: {gsheet_id}")

    data = get_data_from_gsheet(gsheet_id, google_ssm_path, sheet_name)
    logger.info("Data extracted from Google Sheet")

    orders_df = pd.DataFrame(data)
    orders_df.columns = [column.upper() for column in orders_df.columns]

    # Establish Snowflake connection
    database = config["snowflake"]["database"]
    schema = config["snowflake"]["bronze_schema"]
    table = "ORDERS"

    snowflake_conn = establish_snowflake_connection()

    try:
        snowflake_max_timestamp = get_last_transaction_timestamp(
            snowflake_conn, database, schema, table
        )

        orders_df = get_new_orders(orders_df, snowflake_max_timestamp)

        # Load dataframe to Snowflake
        if orders_df.empty:
            logger.info("No new Google Sheets data to extract")
            return

        orders_df["LOADED_AT"] = pd.Timestamp.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        write_dataframe_to_snowflake(
            df=orders_df,
            table_name=table,
            database=database,
            schema=schema,
        )
        logger.info("New data written to Snowflake")

    finally:
        snowflake_conn.close()
        logger.info("Snowflake connection closed")
