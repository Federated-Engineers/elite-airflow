from datetime import datetime
from airflow.sdk import Variable

import awswrangler as wr
import boto3

from .get_all_sheet_record import read_google_sheet


def write_to_user_growth_s3():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    raw_s3_bucket = "federated-engineers-staging-elite-data-lake"
    raw_path_dir = "user_growth_data"
    path = f"s3://{raw_s3_bucket}/{raw_path_dir}/{now}"
    
    wr.engine.set("python")
    
    boto3.setup_default_session(
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
        region_name="eu-central-1"
    )
    
    wr.s3.to_parquet(
        df=read_google_sheet("1xBazE0xUO0sUc7TqsosSCTqe2eQQ1x9-1k5wJemTxc0"),
        path=path,
        dataset=True,
        mode="append"
    )
