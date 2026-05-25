import logging
from datetime import datetime, timezone

import awswrangler as wr
import pandas as pd
from airflow.sdk import Variable

from plugins.date_utils import get_current_datetime
from plugins.google_sheet import get_data_from_gsheet

logger = logging.getLogger(__name__)
config = Variable.get("S3_CONFIG", deserialize_json=True)


SHEET_ID = config["sheet_id"]
SSM_PATH = config["ssm_path"]
RAW_BUCKET = config["raw_bucket"]
DATABASE = config["database"]

wr.engine.set("python")


def extract_sheet(sheet_name: str, table_name: str, s3_prefix: str):
    """
    Extract data from a specific Google Sheet tab
    and write to S3 bucket and Glue catalog.

    Args:
        sheet_name (str): Name of the sheet/tab in the Google Sheet workbook
        table_name (str): Glue table name where the data will be loaded
        s3_prefix (str): S3 path/folder inside the bucket

    Returns:
        None
    """
    logger.info(f"Starting extraction for {sheet_name}")
    execution_date = get_current_datetime().split("_")[0]

    data = get_data_from_gsheet(
        SHEET_ID,
        SSM_PATH,
        sheet_name=sheet_name,
    )
    df = pd.DataFrame(data)

    if df.empty:
        logger.warning(f"No data found in {sheet_name}")
        return df
    df["ingestion_timestamp"] = pd.to_datetime(datetime.now(timezone.utc))
    df["date"] = execution_date
    s3_path = f"s3://{RAW_BUCKET}/{s3_prefix}/"
    wr.s3.to_parquet(
        df=df,
        path=s3_path,
        dataset=True,
        mode="overwrite_partitions",
        table=table_name,
        database=DATABASE,
        partition_cols=["date"],
        dtype={"ingestion_timestamp": "timestamp"}
    )

    logger.info(
        f"{sheet_name} data successfully written to S3 path: {s3_path} "
        f"and Glue table: {table_name}"
    )


def transform_dim_product():
    """
    Function to transform products into a dimension table.
    """
    logger.info("Starting transformation for dim_product")
    execution_date = get_current_datetime().split("_")[0]

    df = wr.s3.read_parquet(f"s3://{RAW_BUCKET}/raw/products/")

    if df.empty:
        logger.warning("No product data found")
        return df

    df["date"] = execution_date
    dim_product = df[
        [
            "product_id",
            "part_name",
            "category",
            "unit_price_euro",
            "date",
        ]
    ]
    path = f"s3://{RAW_BUCKET}/curated/dim/products/"
    wr.s3.to_parquet(
        df=dim_product,
        path=path,
        dataset=True,
        mode="overwrite",
        database=DATABASE,
        table="dim_product",
        partition_cols=["date"],
    )

    logger.info("dim_product successfully written to curated layer")


def transform_fact_orders():
    """
    Function to merge and transform orders and payments into a fact table.
    """
    logger.info("Starting extraction for transform fact")
    execution_date = get_current_datetime().split("_")[0]
    df_orders = wr.s3.read_parquet(f"s3://{RAW_BUCKET}/raw/orders/")
    df_payments = wr.s3.read_parquet(f"s3://{RAW_BUCKET}/raw/payments/")

    if df_orders.empty:
        logger.warning("No orders data found")
        return df_orders

    df_orders["date"] = execution_date
    df_payments["date"] = execution_date

    fact_orders = df_orders.merge(
        df_payments,
        on=["order_id"],
        how="left",
    )
    fact_orders["date"] = execution_date
    path = f"s3://{RAW_BUCKET}/curated/fact/orders/"
    wr.s3.to_parquet(
        df=fact_orders,
        path=path,
        dataset=True,
        mode="overwrite",
        database=DATABASE,
        table="fact_orders",
        partition_cols=["date"],
    )
    logger.info("fact_orders successfully written to curated layer")
