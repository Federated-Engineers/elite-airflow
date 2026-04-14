import logging

import awswrangler as wr
import pandas as pd

from plugins.date_utils import get_current_datetime
from plugins.google_sheet import get_data_from_gsheet
from plugins.pandas_helper import (add_ingestion_timestamp,
                                   convert_columns_to_datetime)
from plugins.s3_helper import write_dataframe_to_s3

logger = logging.getLogger(__name__)


def gsheet_to_s3_dataset(
    gsheet_id: str,
    ssm_path: str,
    bucket: str,
    path_dir: str,
    database: str,
    table_name: str,
    date_column: str,
):
    data = get_data_from_gsheet(gsheet_id, ssm_path)
    df = pd.DataFrame(data)

    df = convert_columns_to_datetime(df, [date_column])
    df = add_ingestion_timestamp(df)

    df["year"] = df[date_column].dt.year
    df["month"] = df[date_column].dt.month
    df["day"] = df[date_column].dt.day

    # wr.engine.set("python")
    path = f"s3://{bucket}/{path_dir}"
    now = get_current_datetime()

    year = df["year"].iloc[0]
    month = df["month"].iloc[0]
    day = df["day"].iloc[0]

    partition_path = f"{path}/year={year}/month={month}/day={day}/"

    try:
        existing_df = wr.s3.read_parquet(path=partition_path)
        logger.info(f"Loaded existing data from {partition_path}")
    except Exception:
        existing_df = pd.DataFrame()
        logger.info("No existing data found, treating as first load")

    if not existing_df.empty:
        df_sorted = (
            df.sort_values(by=df.columns.tolist())
            .reset_index(drop=True)
        )

        existing_sorted = (
            existing_df.sort_values(by=existing_df.columns.tolist())
            .reset_index(drop=True)
        )

        if df_sorted.equals(existing_sorted):
            logger.info("No changes detected. Skipping write to S3.")
            return

    write_dataframe_to_s3(
        df=df,
        path=path,
        partition_cols=["year", "month", "day"],
        database=database,
        table=table_name,
        filename_prefix=f"{now}_",
        mode="overwrite_partitions",
    )

    logger.info("Data written to S3 using overwrite_partitions")
