import awswrangler as wr
import pandas as pd
from airflow.models import Variable

from plugins.date_utils import get_current_datetime
from plugins.google_sheet import get_data_from_gsheet


def write_lancy_to_s3():
    config = Variable.get("hg_config", deserialize_json=True)

    bucket_name = config["bucket_name"]
    folder_name = config["folder_name"]
    ssm_path = config["ssm_path"]

    spreadsheet_id = Variable.get("hg_lancy_sheet_id")

    current_time = get_current_datetime()

    s3_path = (
        f"s3://{bucket_name}/{folder_name}/raw/lancy/"
        f"lancy_{current_time}.parquet"
    )

    data = get_data_from_gsheet(spreadsheet_id, ssm_path)
    df = pd.DataFrame(data)

    wr.s3.to_parquet(df, s3_path)


def write_rhone_to_s3():
    config = Variable.get("hg_config", deserialize_json=True)

    bucket_name = config["bucket_name"]
    folder_name = config["folder_name"]
    ssm_path = config["ssm_path"]

    spreadsheet_id = Variable.get("hg_rhone_sheet_id")

    current_time = get_current_datetime()

    s3_path = (
        f"s3://{bucket_name}/{folder_name}/raw/rhone/"
        f"rhone_{current_time}.parquet"
    )

    data = get_data_from_gsheet(spreadsheet_id, ssm_path)
    df = pd.DataFrame(data)

    wr.s3.to_parquet(df, s3_path)

