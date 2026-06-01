import logging

from airflow.sdk import Variable

from plugins.s3_helper import (read_latest_data_from_s3,
                               write_dataframe_to_s3_glue)

logger = logging.getLogger(__name__)

config = Variable.get("liffey_luxury_config", deserialize_json=True)
bucket = config["s3"]["bucket_name"]


def read_and_join_data():
    """Reads the latest marketing and orders data from S3, performs
    a left join on customer_id, and returns the joined
    DataFrame.
    """

    marketing_path = config["s3"]["marketing_path"]
    orders_path = config["s3"]["orders_path"]

    logger.info("Reading latest marketing data in S3.")
    marketing_data = read_latest_data_from_s3(bucket, marketing_path)

    logger.info("Reading latest orders data in S3.")
    orders_data = read_latest_data_from_s3(bucket, orders_path)

    logger.info("Joining marketing and orders data.")

    df_joined = orders_data.merge(
        marketing_data, on="customer_id", how="left")

    return df_joined


def check_and_write_data_to_s3():
    """Checks if the joined data has changed compared to the current
    version in S3. If there are changes, it writes the new joined
    data to S3. Otherwise it skips the write operation.
    """

    logger.info("Reading latest joined data in S3.")

    current_joined_path = config["s3"]["joined_path"]
    current_joined_data = read_latest_data_from_s3(
        bucket,
        current_joined_path
    )

    incoming_joined_data = read_and_join_data()
    if incoming_joined_data.equals(current_joined_data):
        logger.info("No new data to write to S3.")

    else:
        file_name = "joined.parquet"
        logger.info("Changes detected. Writing joined data to S3.")

        incoming_joined_path = (f"s3://{config['s3']['bucket_name']}"
                                f"/{config['s3']['joined_path']}/")

        write_dataframe_to_s3_glue(df=incoming_joined_data,
                                   path=incoming_joined_path,
                                   database=config["glue"]["database"],
                                   table=config["glue"]["table"],
                                   filename_prefix=file_name,
                                   mode="overwrite")

        logger.info("Data join and upload complete.")
