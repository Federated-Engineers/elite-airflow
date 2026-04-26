import logging

import awswrangler as wr
from airflow.models import Variable

from plugins.date_utils import get_current_datetime
from plugins.s3_helper import get_latest_s3_file, write_dataframe_to_s3_glue

logger = logging.getLogger(__name__)


def get_variable():
    """Get all necessary variables from Airflow Variables"""

    config = Variable.get("liffey_luxury_config", deserialize_json=True)
    if not config:
        logger.error("Liffey_luxury_config is missing.")
        raise ValueError("Configuration variables are missing")

    return config


def transform_and_push_to_s3():
    """Read the latest marketing and orders data from S3,
    joins them using customer_id, and writes the transformed
    data back to S3 (transformed folder) in Parquet format.
    """

    logger.info("Getting latest marketing data from S3.")

    config = get_variable()
    bucket = config["s3"]["bucket_name"]
    marketing_rel_path = config["s3"]["marketing_rpath"]
    orders_rel_path = config["s3"]["orders_rpath"]

    marketing_path = get_latest_s3_file(bucket, marketing_rel_path)
    df_marketing = wr.s3.read_parquet(marketing_path)

    logger.info("Getting latest orders data from S3.")

    orders_path = get_latest_s3_file(bucket, orders_rel_path)
    df_orders = wr.s3.read_parquet(orders_path)

    logger.info("Transforming data.")
    df_transformed = df_orders.merge(df_marketing, on="customer_id",
                                     how="left")

    logger.info("Writing transformed data to S3.")

    file_name = f"{get_current_datetime()}_transformed.parquet"
    write_dataframe_to_s3_glue(df=df_transformed,
                               path=config["s3"]["transformed_path"],
                               database=config["glue"]["database"],
                               table=config["glue"]["table"],
                               filename_prefix=file_name)

    logger.info("Data transformation and upload complete.")
