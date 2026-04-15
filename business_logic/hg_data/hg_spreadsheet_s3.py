import logging
from datetime import datetime

import awswrangler as wr
import pandas as pd
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

from plugins.date_utils import get_current_datetime
from plugins.google_sheet import get_data_from_gsheet

logger = logging.getLogger(__name__)

config = Variable.get("hg_config", deserialize_json=True)

BUCKET_NAME = config["bucket_name"]
FOLDER_NAME = config["folder_name"]
SSM_PATH = config["ssm_path"]
GLUE_DATABASE = config["database"]


def write_sheet_to_s3(
    sheet_id_variable: str,
    dataset_name: str,
    table_name: str
):

    sheet_data = get_data_from_gsheet(
        Variable.get(sheet_id_variable),
        SSM_PATH
    )

    df = pd.DataFrame(sheet_data)

    if df.empty:
        raise AirflowSkipException(
            f"No data found for {dataset_name}."
        )

    execution_date = datetime.strptime(
      get_current_datetime(),
      "%Y-%m-%d_%H:%M:%S")

    df["year"] = execution_date.year
    df["month"] = execution_date.month
    df["day"] = execution_date.day

    wr.config.engine = "python"

    base_path = f"s3://{BUCKET_NAME}/{FOLDER_NAME}/raw/{dataset_name}/"

    hg_partition_path = (
        f"{base_path}"
        f"year={execution_date.year}/"
        f"month={execution_date.month}/"
        f"day={execution_date.day}/"
    )

    try:
        existing_df = wr.s3.read_parquet(path=hg_partition_path)
        logger.info(f"Loaded existing partition: {hg_partition_path}")
    except Exception:
        existing_df = pd.DataFrame()
        logger.info("No existing partition found in hg_data.")

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
            return {
                "status": "no_changes",
                "message": f"No changes detected for {dataset_name}"
            }

    wr.s3.to_parquet(
        df=df,
        path=base_path,
        dataset=True,
        database=GLUE_DATABASE,
        table=table_name,
        mode="overwrite_partitions",
        partition_cols=["year", "month", "day"],
        use_threads=True,
    )
    logger.info("Data appended to S3 dataset.")
