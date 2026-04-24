import logging

import awswrangler as wr

from plugins.date_utils import get_current_datetime
from plugins.s3_helper import get_latest_s3_file, write_df_to_s3

logger = logging.getLogger(__name__)

BUCKET_NAME = "federated-engineers-staging-elite-data-lake"
FOLDER = "liffey_luxury"


def transform_and_push_to_s3():
    """Read the latest marketing and orders data from S3,
    joins them using customer_id, and writes the transformed
    data back to S3 (transformed folder) in Parquet format.
    """

    logger.info("Getting latest marketing data from S3.")
    path = get_latest_s3_file(BUCKET_NAME, f"{FOLDER}/raw/marketing")
    df_marketing = wr.s3.read_parquet(path=path)

    logger.info("Getting latest orders data from S3.")
    path = get_latest_s3_file(BUCKET_NAME, f"{FOLDER}/raw/orders")
    df_orders = wr.s3.read_parquet(path=path)

    logger.info("Transforming data.")
    df_transformed = df_orders.merge(df_marketing, on="customer_id",
                                     how="left")

    logger.info("Writing transformed data to S3.")
    write_df_to_s3(df=df_transformed,
                   bucket_name=BUCKET_NAME,
                   folder_name=f"{FOLDER}/transformed",
                   file_name=f"{get_current_datetime()}_transformed.parquet",
                   # dataset=True,
                   # database="elite-liffey-luxury",
                   # table="transformed_data"
                   )

    logger.info("Data transformation and upload complete.")
