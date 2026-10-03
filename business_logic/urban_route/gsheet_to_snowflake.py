import logging

import pandas as pd
from airflow.models import Variable
from plugins.google_sheet import get_data_from_gsheet
from plugins.snowflake_helper import (establish_snowflake_connection,
                                      write_dataframe_to_snowflake)

logger = logging.getLogger(__name__)

config = Variable.get("urban_route_config", deserialize_json=True)
sensitive_config = Variable.get("urban_route_sensitive_config",
                                deserialize_json=True)


def get_last_payment_timestamp(connection, database, schema, table):
    """ Get last/max payment timestamp from payment table in Snowflake."""

    cursor = connection.cursor()

    try:
        table_path = f'"{database}"."{schema}"."{table}"'

        cursor.execute(
            f"""
            SELECT MAX(PAYMENT_TIMESTAMP)
            FROM {table_path};
            """
        )

        snowflake_max_timestamp = cursor.fetchone()[0]
        logger.info(
            "Last payment timestamp in Snowflake: %s",
            snowflake_max_timestamp,
        )

        return snowflake_max_timestamp

    finally:
        cursor.close()


def get_new_payments(payment_df, snowflake_max_timestamp):
    """Check for Google Sheets data created since the last Snowflake load."""

    # Handles first full load
    if snowflake_max_timestamp is None:
        logger.info(f"Full load to Snowflake {len(payment_df)}")
        return payment_df

    # Convert Google Sheet timestamp to datetime
    payment_df["PAYMENT_TIMESTAMP"] = pd.to_datetime(
        payment_df["PAYMENT_TIMESTAMP"]
    )
    # Implements incremental load
    new_payments = payment_df[
        payment_df["PAYMENT_TIMESTAMP"] > snowflake_max_timestamp
    ].copy()
    logger.info(f"{len(new_payments)} new data found")

    return new_payments


def gsheets_to_snowflake():
    """Extract daily payments data from a Google Sheet and write to Snowflake.

    All variables needed for this function are retrieved from Airflow Variables
    The variables include Google Sheet ID, SSM path for Google credentials
    """

    gsheet_id = config["google_sheet"]["sheet_id"]
    sheet_name = config["google_sheet"]["sheet_name"]
    google_ssm_path = sensitive_config["google_ssm_path"]

    logger.info(f"Connecting to Google Sheet with ID: {gsheet_id}")

    data = get_data_from_gsheet(gsheet_id, google_ssm_path, sheet_name)
    logger.info("Data extracted from Google Sheet")

    payment_df = pd.DataFrame(data)
    payment_df.columns = [column.upper() for column in payment_df.columns]

    # Establish Snowflake connection
    database = config["snowflake"]["database"]
    schema = config["snowflake"]["raw"]
    table = "PAYMENTS"

    snowflake_conn = establish_snowflake_connection()

    try:
        snowflake_max_timestamp = get_last_payment_timestamp(
            snowflake_conn, database, schema, table
        )

        payment_df = get_new_payments(payment_df, snowflake_max_timestamp)

        # Load dataframe to Snowflake
        if payment_df.empty:
            logger.info("No new Google Sheets data to extract")
            return

        payment_df["LOADED_AT"] = pd.Timestamp.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        write_dataframe_to_snowflake(
            df=payment_df,
            table_name=table,
            database=database,
            schema=schema,
        )
        logger.info("New data written to Snowflake")

    finally:
        snowflake_conn.close()
        logger.info("Snowflake connection closed")

