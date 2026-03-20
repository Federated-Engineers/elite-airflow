import logging

from plugins.gsheets_auth import get_google_sheets_client

logger = logging.getLogger(__name__)


def get_data_from_gsheet(gsheet_id: str):
    """Ingest all records from the first worksheet of a Google Sheet.

    Args:
        gsheet_id: The Google Sheet ID/key.
    Returns:
        A list of records from the sheet.
    """

    logger.info("Starting data ingestion from Google Sheet")

    gc = get_google_sheets_client()
    workbook = gc.open_by_key(gsheet_id)
    worksheet = workbook.sheet1

    data = worksheet.get_all_records()
    logger.info("Data ingestion completed")

    return data
