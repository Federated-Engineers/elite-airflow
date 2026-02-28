import io
import boto3
import pandas as pd
from business_logic.nordic_ws.config.sheets import S3_BUCKET, AWS_REGION
from airflow.sdk.bases.hook import BaseHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

def get_s3_client():
    hook = AwsBaseHook(aws_conn_id="aws_default", client_type="s3")
    return hook.get_client_type(region_name=AWS_REGION)


def read_parquet(s3_key):
    """Read existing parquet from S3, return empty df if not found."""
    s3 = get_s3_client()
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        return pd.read_parquet(io.BytesIO(obj["Body"].read()))
    except s3.exceptions.NoSuchKey:
        return pd.DataFrame()
    except Exception as e:
        print(f"  Warning reading {s3_key}: {e}")
        return pd.DataFrame()

def write_parquet(df, s3_key):
    """Write dataframe as parquet to S3."""
    s3 = get_s3_client()
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )
    print(f"  Written to s3://{S3_BUCKET}/{s3_key}")