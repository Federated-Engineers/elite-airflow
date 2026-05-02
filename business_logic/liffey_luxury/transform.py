import logging

import awswrangler as wr
import pandas as pd
from airflow.sdk import Variable

from plugins.date_utils import get_current_datetime
from plugins.s3_helper import (read_latest_data_from_s3,
                               write_dataframe_to_s3_glue)

logger = logging.getLogger(__name__)

config = Variable.get("liffey_luxury_config", deserialize_json=True)
bucket = config["s3"]["bucket_name"]


def read_and_join_data():
    marketing_path = config["s3"]["marketing_path"]
    orders_path = config["s3"]["orders_path"]

    logger.info("Reading latest marketing data in S3.")
    marketing_data = read_latest_data_from_s3(bucket, marketing_path)

    logger.info("Reading latest orders data in S3.")
    orders_data = read_latest_data_from_s3(bucket, orders_path)

    logger.info("Joining marketing and orders data.")
    df_transformed = orders_data.merge(marketing_data, on="customer_id",
                                        how="left")
    
    return df_transformed


def check_and_write_data_to_s3():
    logger.info("Reading latest transformed data in S3.")

    current_transformed_path = config["s3"]["transformed_path"]
    current_transformed_data = read_latest_data_from_s3(
        bucket,
        current_transformed_path
    )
    
    incoming_transformed_data = read_and_join_data()
    if incoming_transformed_data.equals(current_transformed_data):
        logger.info("No new data to write to S3.")
        return
    
    file_name = f"{get_current_datetime()}_transformed.parquet"
    logger.info("Changes detected. Writing transformed data to S3.")

    incoming_transformed_path = (f"s3://{config['s3']['bucket_name']}"
                                 f"/{config['s3']['transformed_path']}/")

    write_dataframe_to_s3_glue(df=incoming_transformed_data,
                                path=incoming_transformed_path,
                                database=config["glue"]["database"],
                                table=config["glue"]["table"],
                                filename_prefix=file_name)

    logger.info("Data transformation and upload complete.")
