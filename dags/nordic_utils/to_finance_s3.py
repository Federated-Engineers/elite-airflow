from datetime import datetime

import awswrangler as wr
import boto3
from airflow.sdk import Variable

from .get_all_sheet_record import read_google_sheet


def write_to_finance_s3():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    raw_s3_bucket = "federated-engineers-staging-elite-data-lake"
    raw_path_dir = "finance_data"
    path = f"s3://{raw_s3_bucket}/{raw_path_dir}/{now}"
    wr.engine.set("python")
    boto3.setup_default_session(
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
        region_name="eu-central-1"
    )
    wr.s3.to_parquet(
        df=read_google_sheet("1vQFb4E7QEpVRkpRe34CyGfaH9VJRFVZpFLAc7taYYwg"),
        path=path,
        dataset=True,
        mode="append"
    )
