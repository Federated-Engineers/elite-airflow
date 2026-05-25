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

LANCY_SHEET_ID = config["hg_lancy_sheet_id"]
RHONE_SHEET_ID = config["hg_rhone_sheet_id"]


def write_sheet_to_s3(
    sheet_id: str,
    dataset_name: str,
    table_name: str
):

    sheet_data = get_data_from_gsheet(
        sheet_id,
        SSM_PATH
    )

    df = pd.DataFrame(sheet_data)

    if df.empty:
        raise AirflowSkipException(
            f"No data found for {dataset_name}."
        )

    execution_date = datetime.strptime(
        get_current_datetime(),
        "%Y-%m-%d_%H:%M:%S"
    )

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
    except Exception:
        existing_df = pd.DataFrame()

    if existing_df.empty:
        pass
    elif df.sort_values(df.columns.tolist()).reset_index(drop=True).equals(
        existing_df.sort_values(
            existing_df.columns.tolist()).reset_index(drop=True)
    ):
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

    logger.info(f"Data written to S3 dataset: {dataset_name}")
