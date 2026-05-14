import json
import logging

import pandas as pd
import psycopg2
from plugins.aws import get_ssm_parameter
from plugins.date_utils import get_current_datetime
from plugins.pandas_helper import add_ingestion_timestamp
from plugins.s3_helper import write_dataframe_to_s3_glue

logger = logging.getLogger(__name__)


def path_variable() -> dict:
    filename = get_current_datetime() + "_"
    ingestion_ts = pd.Timestamp.now(tz="UTC")
    ingestion_hive_partition = (
        f"year={ingestion_ts.year}"
        f"/month={ingestion_ts.month:02d}"
        f"/day={ingestion_ts.day:02d}"
    )

    return {
        "bucket_name": "federated-engineers-production-elite-mare-viva-bucket",
        "folder_name_harvest": "harvest_lifecycle_record",
        "folder_name_lagoon": "lagoon_environmental_log",
        "ssm_credentials_link": "/supabase/database/credentials",
        "filename": filename,
        "ingestion_hive_partition": ingestion_hive_partition,
    }


def get_supabase_credentials():
    credentials_dict = json.loads(
        get_ssm_parameter(
            path_variable()["ssm_credentials_link"])
        )
    return {
        "host": credentials_dict["host"],
        "port": 5432,
        "database": credentials_dict["database_name"],
        "user": credentials_dict["username"],
        "password": credentials_dict["password"]
    }


def connect_to_supabase():
    conn = psycopg2.connect(**get_supabase_credentials())
    return conn


def extract_table(schema: str, table_name: str, date_column: str):
    query = f"SELECT * FROM {schema}.{table_name} ORDER BY {date_column};"
    df = pd.read_sql(query, connect_to_supabase())
    return df


def harvest_partition_cols():
    df = add_ingestion_timestamp(
        extract_table("historical", "harvest_lifecycle_record", "seeding_date")
        )
    df["year"] = df["ingestion_timestamp"].dt.year
    df["month"] = df["ingestion_timestamp"].dt.month
    df["day"] = df["ingestion_timestamp"].dt.day
    return df


def lagoon_partition_cols():
    df = add_ingestion_timestamp(
        extract_table("historical", "lagoon_environmental_log", "timestamp")
    )
    df["year"] = df["ingestion_timestamp"].dt.year
    df["month"] = df["ingestion_timestamp"].dt.month
    df["day"] = df["ingestion_timestamp"].dt.day
    return df


def harvest_full_path():
    variables = path_variable()
    bucket = variables['bucket_name']
    folder = variables['folder_name_harvest']
    return f"s3://{bucket}/{folder}"


def lagoon_full_path():
    variables = path_variable()
    bucket = variables['bucket_name']
    folder = variables['folder_name_lagoon']
    return f"s3://{bucket}/{folder}"


def extract_harvest_to_s3_to_glue():
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
