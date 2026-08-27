import logging

import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

logger = logging.getLogger(__name__)


def establish_snowflake_connection(
        snowflake_conn_id: str = "snowflake_default",
        ):
    """Establishes a Snowflake connection managed by an Airflow Connection

    Args:
        snowflake_conn_id: Airflow Connection ID for Snowflake. This is
        inputed directly in the Snowflake UI

    Returns:
        connection: Snowflake connection object.
    """

    logger.info(
        "Connecting to Snowflake using Airflow connection: %s",
        snowflake_conn_id,
    )

    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    connection = hook.get_conn()
    logger.info("Snowflake connection established")

    return connection


def write_dataframe_to_snowflake(
        df: pd.DataFrame,
        table_name: str,
        database: str,
        schema: str,
        snowflake_conn_id: str = "snowflake_default",
        quote_identifiers: bool = False,
        connection=None,
        ):
    """Write a pandas DataFrame to an existing Snowflake table.

    Args:
        df: pandas Dataframe to insert into table
        table: Existing Snowflake destination table to load dataframe into
        database: Destination Snowflake database
        schema: Destination Snowflake schema
        snowflake_conn_id: Airflow Connection ID for Snowflake
        quote_identifiers: Whether Snowflake identifiers should be quoted
        connection: An existing Snowflake connection. If not provided, a new
            connection is opened for the write.
    """

    if df.empty:
        raise ValueError("No data to write to Snowflake")

    no_pre_existing_connection = connection is None

    if no_pre_existing_connection:
        connection = establish_snowflake_connection(
            snowflake_conn_id=snowflake_conn_id,
        )

    # Insert dataframe in Snowflake table
    try:
        success, _, num_rows, _ = write_pandas(
            conn=connection,
            df=df,
            table_name=table_name,
            database=database,
            schema=schema,
            quote_identifiers=quote_identifiers,
        )

        if not success:
            raise RuntimeError(
                "Failed to load DataFrame into "
                f"{database}.{schema}.{table_name}"
            )

        logger.info(
            "Successfully loaded %s rows into %s.%s.%s",
            num_rows,
            database,
            schema,
            table_name,
        )

    finally:
        if no_pre_existing_connection:
            connection.close()
            logger.info("Snowflake connection closed")
