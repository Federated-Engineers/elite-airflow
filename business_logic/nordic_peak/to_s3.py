from datetime import datetime

import awswrangler as wr
import boto3
from airflow.sdk import Variable

from google_auth import read_google_sheet



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



def write_to_marketing_s3():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    raw_s3_bucket = "federated-engineers-staging-elite-data-lake"
    raw_path_dir = "marketing_campaign_data"
    path = f"s3://{raw_s3_bucket}/{raw_path_dir}/{now}"
    wr.engine.set("python")
    boto3.setup_default_session(
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
        region_name="eu-central-1"
    )
    wr.s3.to_parquet(
        df=read_google_sheet("13ULiowSb4JuNIaLldstlNB7sqr1WBe0Yrjm8ALEyLQ0"),
        path=path,
        dataset=True,
        mode="append"
    )



def write_to_supply_chain_s3():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    raw_s3_bucket = "federated-engineers-staging-elite-data-lake"
    raw_path_dir = "supply_chain_data"
    path = f"s3://{raw_s3_bucket}/{raw_path_dir}/{now}"
    wr.engine.set("python")
    boto3.setup_default_session(
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
        region_name="eu-central-1"
    )
    wr.s3.to_parquet(
        df=read_google_sheet("1MLhga5_FJpgpr8sBLjUlvwQ8sNDPULnv3Rbps3J5l7k"),
        path=path,
        dataset=True,
        mode="append"
    )



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
