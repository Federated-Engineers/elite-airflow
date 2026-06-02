import json
import logging

from airflow.models import Variable

from plugins.aws import get_ssm_parameter
from plugins.database import (postgres_db_connection,
                              postgres_query_output_to_df)
from plugins.pandas_helper import add_ingestion_timestamp, table_partition_cols

logger = logging.getLogger(__name__)


def get_config():
    """
    Retrieve the DAG configuration from Airflow Variables.

    Returns:
        dict: Configuration parameters including:
            - bucket_name: S3 bucket name.
            Update this to your actual production bucket.
            - folder_name_harvest: S3 folder for harvest data.
            - folder_name_lagoon: S3 folder for lagoon data.
            - glue_production_db: Production Glue database name.
            if production db is not used, you can ignore this parameter.
            - staging_glue_db: Staging Glue database name.
            As with production db, if staging db is not used,
            ignore this parameter.
            - ssm_credentials_link: SSM path to DB credentials.
    """
    return Variable.get("config", deserialize_json=True)
# Update "config" to your actual Airflow Variable name if different.


def get_db_connection():
    """
    Establish a PostgreSQL connection using credentials from SSM.

    Credentials are stored in SSM as a JSON string with keys:
        host, database_name, username, password, port (optional).

    Returns:
        connection: psycopg2 connection object.
    """
    db_cred = json.loads(
        get_ssm_parameter(get_config()["ssm_credentials_link"])
    )
    return postgres_db_connection(db_cred)


def extract_data():
    """
    Extract data from the PostgreSQL database.

    Queries harvest_lifecycle_record and lagoon_environmental_log
    from the historical schema.

    Returns:
        dict: Contains two DataFrames:
            - harvest_all_data: Records from harvest_lifecycle_record.
            - lagoon_all_data: Records from lagoon_environmental_log.
    """
    harvest_all_data = postgres_query_output_to_df(
        "historical", "harvest_lifecycle_record", get_db_connection()
    )
    lagoon_all_data = postgres_query_output_to_df(
        "historical", "lagoon_environmental_log", get_db_connection()
    )
    return {
        "harvest_all_data": harvest_all_data,
        "lagoon_all_data": lagoon_all_data,
    }


def transform_data():
    """
    A function that transforms the extracted data by adding ingestion
    timestamps and partition columns for both harvest
    and lagoon datasets.
    Args:
        None
    Returns:
        dict: Contains two DataFrames:
            - harvest_date_partitioned: Harvest data with timestamps
              and partitions.
            - lagoon_date_partitioned: Lagoon data with timestamps
              and partitions.
    """
    data_dict = extract_data()
    harvest_all_data = data_dict["harvest_all_data"]
    lagoon_all_data = data_dict["lagoon_all_data"]

    harvest_with_timestamp = add_ingestion_timestamp(harvest_all_data)
    lagoon_with_timestamp = add_ingestion_timestamp(lagoon_all_data)

    harvest_date_partitioned = table_partition_cols(harvest_with_timestamp)
    lagoon_date_partitioned = table_partition_cols(lagoon_with_timestamp)

    return {
        "harvest_date_partitioned": harvest_date_partitioned,
        "lagoon_date_partitioned": lagoon_date_partitioned,
    }


def load_data_to_s3_glue():
    """
    Load transformed data to S3 and register it in AWS Glue Catalog.

    Returns:
        dict: Contains two DataFrames:
            - harvest_date_partitioned_df: Harvest data for S3/Glue.
            - lagoon_date_partitioned_df: Lagoon data for S3/Glue.
    """
    data_dict = transform_data()
    return {
        "harvest_date_partitioned_df": data_dict["harvest_date_partitioned"],
        "lagoon_date_partitioned_df": data_dict["lagoon_date_partitioned"],
    }
