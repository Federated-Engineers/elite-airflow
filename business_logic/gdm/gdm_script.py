import awswrangler as wr
import boto3
from airflow.models import Variable
from datetime import datetime, timezone
import logging

logger=logging.getLogger(__name__)

bucket = "gdm-raw-data"
folder = "daily_extracts"
s3_path = f"s3://{bucket}/{folder}"


def extract_portugal_data():

    s3 = boto3.client("s3")
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=f"{folder}/")

    files = response.get("Contents", [])

    if len(files) == 0:
        raise ValueError(f"No file found in {s3_path}")

    backfilled_object=Variable.get("backfill_file", default_var=None)
    if backfilled_object:
        logger.info(f"Oh there is backfilled file {backfilled_object}")
        df = wr.s3.read_parquet(f"s3://{backfilled_object}")
        df_portugal = df[df["plant_country"] == "Portugal"]

        Variable.delete("backfill_file")
        logger.info("Backfill file processed and variable cleared")
    else:

        latest_object = max(files, key=lambda x: x["LastModified"])
        latest_file = f"s3://{bucket}/{latest_object['Key']}"
        logger.info(f"Latest file: {latest_file}")

        last_modified = latest_object["LastModified"]

        today= datetime.now(timezone.utc)
        date_difference = (today.date() - last_modified.date()).days

        if date_difference > 1:
            logger.info("No new file to process today.")
            return None
        
        df = wr.s3.read_parquet(latest_file)
        df_portugal = df[df["plant_country"] == "Portugal"]
        logger.info("Portugal data extracted successfully.")

    return df_portugal
