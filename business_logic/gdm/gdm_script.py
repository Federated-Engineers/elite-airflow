import logging
from datetime import datetime, timezone

import logging
from datetime import datetime, timezone

import awswrangler as wr
import boto3
from airflow.sdk import Variable
from business_logic.gdm.google_sheets import append_dataframe_to_sheet

logger = logging.getLogger(__name__)

bucket = "gdm-raw-data"
folder = "daily_extracts"
s3_path = f"s3://{bucket}/{folder}"


def extract_portugal_data(spreadsheet_id, worksheet_name):
    """Extracts Portugal data from the latest parquet file in S3 and appends
    it to a Google Sheet. If a backfill file is specified in Airflow Variables,
    it processes that file instead.

    Args:
        spreadsheet_id: The ID of the Google Sheet to append data to
        worksheet_name: The name of the worksheet within the Google Sheet
        to append data to.
    Returns:
        A DataFrame containing the Portugal data that was processed and
        written to Google Sheets.
    """

    logger.info("Starting Portugal data extraction pipeline")
def extract_portugal_data(spreadsheet_id, worksheet_name):
    """Extracts Portugal data from the latest parquet file in S3 and appends
    it to a Google Sheet. If a backfill file is specified in Airflow Variables,
    it processes that file instead.

    Args:
        spreadsheet_id: The ID of the Google Sheet to append data to
        worksheet_name: The name of the worksheet within the Google Sheet
        to append data to.
    Returns:
        A DataFrame containing the Portugal data that was processed and
        written to Google Sheets.
    """

    logger.info("Starting Portugal data extraction pipeline")

    s3 = boto3.client("s3")
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=f"{folder}/"
    )

    files = response.get("Contents", [])

    if len(files) == 0:
        raise ValueError(f"No file found in {s3_path}")
    logger.info(f"{len(files)} file(s) found in {s3_path}")

    backfilled_object = Variable.get("backfill_file", default=None)

    if backfilled_object:
        logger.info(f"Backfill detected. Processing file: {backfilled_object}")

        file_path = f"s3://{backfilled_object}"
        logger.info(f"Reading parquet file from {file_path}")

        df = wr.s3.read_parquet(file_path)
        df_portugal = df[df["plant_country"] == "Portugal"]
        logger.info(f"{len(df_portugal)} Backfill Portugal rows extracted")

        Variable.delete("backfill_file")
        logger.info("Backfill file processed and variable cleared")

    else:
        latest_object = max(files, key=lambda x: x["LastModified"])
        latest_file = f"s3://{bucket}/{latest_object['Key']}"
        logger.info(f"Latest file: {latest_file}")

        last_modified = latest_object["LastModified"]

        today = datetime.now(timezone.utc)
        date_difference = (today.date() - last_modified.date()).days

        if date_difference > 13:
            logger.info("No new file to process today.")
            return None

        df = wr.s3.read_parquet(latest_file)
        logger.info("Reading recent file completed")

        df_portugal = df[df["plant_country"] == "Portugal"]
        logger.info(f"{len(df_portugal)} Portugal data extracted successfully")

    if df_portugal.empty:
        logger.warning("No Portugal data found in the file.")
        return df_portugal

    logger.info("Writing Portugal data to Google Sheets")
    append_dataframe_to_sheet(
        df_portugal,
        spreadsheet_id,
        worksheet_name
    )

    logger.info("Portugal data successfully written to Google Sheets")
    return df_portugal
