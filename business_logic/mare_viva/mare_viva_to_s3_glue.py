import json
import logging

import pandas as pd
import psycopg2
from airflow.models import Variable

from plugins.aws import get_ssm_parameter
from plugins.date_utils import get_current_datetime
from plugins.pandas_helper import add_ingestion_timestamp
from plugins.s3_helper import write_dataframe_to_s3_glue

logger = logging.getLogger(__name__)

my_config = {
    "bucket_name": "federated-engineers-production-elite-mare-viva-bucket",
    "folder_name_harvest": "harvest_lifecycle_record",
    "folder_name_lagoon": "lagoon_environmental_log",
    "ssm_credentials_link": "/supabase/database/credentials",
}

def hive_partition_vars() -> dict:
    """
    A function that generates Hive partition variables based on the current date and time.
    Args:
        None
    Returns:
        dict: A dictionary containing the filename and ingestion Hive partition path.
    """
    current_datetime = get_current_datetime()
    year, month, day = current_datetime.split("_")[0].split("-")

    filename = f"{current_datetime}_"
    ingestion_hive_partition = f"year={year}/month={month}/day={day}"

    path_vars = {
        "filename": filename,
        "ingestion_hive_partition": ingestion_hive_partition,
    }
    return path_vars


def get_supabase_credentials() -> dict:
    """
    Retrieves Supabase database credentials from AWS SSM Parameter Store.
    Args:
        None
    Returns:
        A dictionary containing the database connection parameters.
    """
    ssm_path = path_variable()["ssm_credentials_link"]
    raw_credentials = get_ssm_parameter(ssm_path)
    credentials = json.loads(raw_credentials)

    connection_params = {
        "host": credentials["host"],
        "port": 5432,
        "database": credentials["database_name"],
        "user": credentials["username"],
        "password": credentials["password"],
    }
    return connection_params


def connect_to_supabase():
    """
    A function that connects to the Supabase database.
    Args:
        None
    Returns:
        A connection object for the Supabase database.
    """
    conn = psycopg2.connect(**get_supabase_credentials())
    return conn


def extract_table(schema: str, table_name: str, date_column: str):
    """
    A function that extracts data from a specified table
    in the Supabase database.
    Args:
        schema (str): The schema name.
        table_name (str): The table name.
        date_column (str): The column to order by.
    Returns:
        pd.DataFrame: The extracted data as a pandas DataFrame.
    """
    query = f"SELECT * FROM {schema}.{table_name} ORDER BY {date_column};"
    df = pd.read_sql(query, connect_to_supabase())
    return df


def harvest_partition_cols():
    """
    A function that adds partition columns for the harvest data.
    Args:
        None
    Returns:
        pd.DataFrame: The DataFrame with partition columns added.
    """
    df = add_ingestion_timestamp(
        extract_table("historical", "harvest_lifecycle_record", "seeding_date")
        )
    df["year"] = df["ingestion_timestamp"].dt.year
    df["month"] = df["ingestion_timestamp"].dt.month
    df["day"] = df["ingestion_timestamp"].dt.day
    return df


def lagoon_partition_cols():
    """A function that adds partition columns for the lagoon data.
    Args:
        None
    Returns:
        pd.DataFrame: The DataFrame with partition columns added.
    """
    df = add_ingestion_timestamp(
        extract_table("historical", "lagoon_environmental_log", "timestamp")
    )
    df["year"] = df["ingestion_timestamp"].dt.year
    df["month"] = df["ingestion_timestamp"].dt.month
    df["day"] = df["ingestion_timestamp"].dt.day
    return df


def harvest_full_path():
    """A function that constructs the full S3 path for the harvest data.
    Args:
        None
    Returns:
        str: The full S3 path for the harvest data.
    """
    variables = path_variable()
    bucket = variables['bucket_name']
    folder = variables['folder_name_harvest']
    return f"s3://{bucket}/{folder}"


def lagoon_full_path():
    """A function that constructs the full S3 path for the lagoon data.
    Args:
        None
    Returns:
        str: The full S3 path for the lagoon data.
    """
    variables = path_variable()
    bucket = variables['bucket_name']
    folder = variables['folder_name_lagoon']
    return f"s3://{bucket}/{folder}"


def extract_harvest_to_s3_to_glue():
    """A function that extracts the harvest data,
        adds partition columns, and writes it to S3
        and to Glue
    Args:
        None
    Returns:
        None
    """
    df = harvest_partition_cols()
    write_dataframe_to_s3_glue(
        df,
        harvest_full_path(),
        ["year", "month", "day"],
        path_variable()["filename"],
        "elite-mare-viva",
        "harvest_lifecycle_record",
        "append",
    )


def extract_lagoon_to_s3_to_glue():
    """A function that extracts the lagoon data,
        adds partition columns, and writes it to S3
        and to Glue.
    Args:
        None
    Returns:
        None
    """
    df = lagoon_partition_cols()
    write_dataframe_to_s3_glue(
        df,
        lagoon_full_path(),
        ["year", "month", "day"],
        path_variable()["filename"],
        "elite-mare-viva",
        "lagoon_environmental_log",
        "append",
    )
