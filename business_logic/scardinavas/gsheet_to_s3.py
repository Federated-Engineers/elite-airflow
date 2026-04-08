import logging
from datetime import datetime

import awswrangler as wr
import pandas as pd

from plugins.date_utils import get_current_datetime
from plugins.google_sheet import get_data_from_gsheet

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

    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
    df["ingestion_timestamp"] = pd.Timestamp.now(tz="UTC")
    df["created_at"] = df["ingestion_timestamp"].dt.date

    df["year"] = df[date_column].dt.year
    df["month"] = df[date_column].dt.month
    df["day"] = df[date_column].dt.day

    path = f"s3://{bucket}/{path_dir}"
    now = get_current_datetime()

    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="append",
        partition_cols=["year", "month", "day"],
        database=database,
        table=table_name,
        compression="snappy",
        filename_prefix=f"{now}_",
        schema_evolution=True,
    )
    