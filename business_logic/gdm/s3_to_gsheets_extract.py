import logging

import awswrangler as wr
from airflow.sdk import Variable

from plugins.append_to_gsheet import append_dataframe_to_sheet
from plugins.s3_helper import get_latest_s3_file

logger = logging.getLogger(__name__)

BUCKET = "gdm-raw-data"
FOLDER = "daily_extracts"


def extract_portugal_data(spreadsheet_id, worksheet_name):
    """Extracts Portugal data from the latest parquet file in S3 and appends
    it to a Google Sheet. If a backfill file is specified in Airflow Variables,
    it processes that file instead.

    Args:
        spreadsheet_id: The ID of the Google Sheet to append data to.
        worksheet_name: The name of the worksheet within the Google Sheet
            to append data to.
    Returns:
        None if no new file to process or appends data Portugal data to
        Google Sheets.
    """

    logger.info("Starting Portugal data extraction pipeline")

    backfilled_object = Variable.get("backfill_file", default=None)

    if backfilled_object:
        logger.info(f"Backfill detected. Processing file: {backfilled_object}")
        file_path = f"s3://{backfilled_object}"

        df = wr.s3.read_parquet(file_path)
        df_portugal = df[df["plant_country"] == "Portugal"]
        logger.info(f"{len(df_portugal)} Backfill Portugal rows extracted")

        Variable.delete("backfill_file")
        logger.info("Backfill file processed and variable cleared")

    else:
        latest_file = get_latest_s3_file(BUCKET, FOLDER, last_date=20)
        if latest_file is None:
            return None

        df = wr.s3.read_parquet(latest_file)
        logger.info("Reading recent file completed")

        df_portugal = df[df["plant_country"] == "Portugal"]
        logger.info(f"{len(df_portugal)} Portugal data extracted successfully")

    if df_portugal.empty:
        logger.warning("No Portugal data found in the file.")

    logger.info("Writing Portugal data to Google Sheets")

    append_dataframe_to_sheet(df_portugal, spreadsheet_id, worksheet_name)
    logger.info("Portugal data successfully written to Google Sheets")
