import io
import boto3
import pandas as pd
from nordic_ws.config.sheets import S3_BUCKET, AWS_REGION

s3 = boto3.client("s3", region_name=AWS_REGION)

def read_parquet(s3_key):
    """Read existing parquet from S3, return empty df if not found."""
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