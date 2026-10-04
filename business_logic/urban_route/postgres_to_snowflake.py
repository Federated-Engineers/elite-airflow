import json
import logging

import pandas as pd
from airflow.models import Variable

from plugins.aws import get_ssm_parameter
from plugins.database import db_query_results_to_df, postgres_db_connection
from plugins.snowflake_helper import (establish_snowflake_connection,
                                      write_dataframe_to_snowflake)

logger = logging.getLogger(__name__)


config = Variable.get(
    "urban_route_config",
    deserialize_json=True,
)

sensitive_config = Variable.get(
    "urban_route_sensitive_config",
    deserialize_json=True,
)


def get_last_value_in_table(
    connection,
    database,
    schema,
    table,
    column,
):
    """Get the maximum watermark value already loaded into Snowflake."""

    cursor = connection.cursor()

    try:
        table_path = f'"{database}"."{schema}"."{table}"'

        cursor.execute(
            f"""
            SELECT MAX("{column}")
            FROM {table_path};
            """
        )

        snowflake_max_value = cursor.fetchone()[0]

        logger.info(
            "Maximum %s in %s is %s",
            column,
            table,
            snowflake_max_value,
        )

        return snowflake_max_value

    finally:
        cursor.close()


def get_new_postgres_data(
    connection,
    table,
    column,
    data_type,
    snowflake_max_value,
):
    """Extract new records from an UrbanRoute PostgreSQL table."""

    query = f"""
        SELECT *
        FROM data_factory.{table}
    """

    if snowflake_max_value is not None:
        query += f"""
        WHERE {column}::{data_type} > '{snowflake_max_value}'
        """

    query += ";"

    logger.info(
        "Extracting data from PostgreSQL table: %s",
        table,
    )

    df = db_query_results_to_df(
        connection=connection,
        query=query,
    )

    df.columns = [
        column.upper()
        for column in df.columns
    ]

    return df


def postgres_to_snowflake():
    """Incrementally ingest UrbanRoute PostgreSQL tables into Bronze."""

    db_ssm_path = sensitive_config["db_ssm_path"]

    db_cred = json.loads(
        get_ssm_parameter(db_ssm_path)
    )

    postgres_conn = postgres_db_connection(db_cred)

    logger.info("Connected to PostgreSQL database")

    database = config["snowflake"]["database"]
    schema = config["snowflake"]["raw"]

    snowflake_conn = establish_snowflake_connection()

    logger.info("Connected to Snowflake")

    # PostgreSQL tables and incremental watermarks
    tables = {
        "USERS": (
            "REGISTRATION_DATE",
            "DATE",
        ),
        "VEHICLES": (
            "DEPLOYMENT_DATE",
            "DATE",
        ),
        "APP_PAYMENTS": (
            "PAYMENT_TIMESTAMP",
            "TIMESTAMP",
        ),
    }

    try:

        for table, (column, data_type) in tables.items():

            # Find where Snowflake currently stops
            snowflake_max_value = get_last_value_in_table(
                connection=snowflake_conn,
                database=database,
                schema=schema,
                table=table,
                column=column,
            )

            # Extract only newer PostgreSQL records
            df = get_new_postgres_data(
                connection=postgres_conn,
                table=table,
                column=column,
                data_type=data_type,
                snowflake_max_value=snowflake_max_value,
            )

            # Nothing new
            if df.empty:
                logger.info(
                    "No new data found in %s",
                    table,
                )
                continue

            # Logging
            if snowflake_max_value is None:
                logger.info(
                    "Initial full load: %s rows from %s",
                    len(df),
                    table,
                )
            else:
                logger.info(
                    "Incremental load: %s new rows from %s",
                    len(df),
                    table,
                )

            # Add ingestion metadata
            df["LOADED_AT"] = pd.Timestamp.now()

            # Load into Snowflake raw
            write_dataframe_to_snowflake(
                df=df,
                table_name=table,
                database=database,
                schema=schema,
            )

            logger.info(
                "%s rows from %s written to Snowflake",
                len(df),
                table,
            )

    finally:

        snowflake_conn.close()
        logger.info("Snowflake connection closed")

        postgres_conn.close()
        logger.info("PostgreSQL connection closed")

