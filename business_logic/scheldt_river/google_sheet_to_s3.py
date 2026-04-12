import logging
from datetime import datetime, timezone

import awswrangler as wr
import pandas as pd
from airflow.sdk import get_current_context

from plugins.google_sheet import get_data_from_gsheet

logger = logging.getLogger(__name__)

SHEET_ID = "1Q5ojWBFb2_5FsJ9VuFqr7PH1NmstbelctUP_azFRLXg"
SSM_PATH = "/production/google-service-account/credentials"
RAW_BUCKET = "federated-engineers-production-elite-scheldt"
DATABASE = "scheldt-production-database"


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
    logger.info("Starting extraction for %s", sheet_name)
    context = get_current_context()
    execution_date = context["ds"]

    data = get_data_from_gsheet(
        SHEET_ID,
        SSM_PATH,
        sheet_name=sheet_name,
    )
    df = pd.DataFrame(data)

    if df.empty:
        logger.warning("No data found in %s", sheet_name)
        return

    wr.engine.set("python")
    df["ingestion_timestamp"] = datetime.now(timezone.utc)
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
    )

    logger.info(
        f"{sheet_name} data successfully written to S3 path: {s3_path} "
        f"and Glue table: {table_name}"
    )


def transform_dim_product():
    """
    Function to transform products into a dimension table.
    """
    logger.info("Starting transform for dim_product")
    context = get_current_context()
    execution_date = context["ds"]

    df = wr.s3.read_parquet(f"s3://{RAW_BUCKET}/raw/products/")

    if df.empty:
        logger.warning("No product data found")
        return

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

    wr.engine.set("python")

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
    context = get_current_context()
    execution_date = pd.to_datetime(context["ds"]).date()

    df_orders = wr.s3.read_parquet(f"s3://{RAW_BUCKET}/raw/orders/")
    df_payments = wr.s3.read_parquet(f"s3://{RAW_BUCKET}/raw/payments/")

    if df_orders.empty:
        logger.warning("No orders data found")
        return

    df_orders["date"] = execution_date
    df_payments["date"] = execution_date

    fact_orders = df_orders.merge(
        df_payments,
        on=["order_id", "date"],
        how="left",
    )
    fact_orders["date"] = execution_date

    wr.engine.set("python")

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
