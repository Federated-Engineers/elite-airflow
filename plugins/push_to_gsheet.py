import logging

from plugins.gsheets_auth import get_google_sheets_client

logger = logging.getLogger(__name__)


def append_dataframe_to_sheet(df, spreadsheet_id: str, worksheet_name: str) -> None:
    """
    Append dataframe rows to a Google Sheet worksheet.

    Args:
        df: The pandas DataFrame to append.
        spreadsheet_id: The ID of the Google Sheet.
        worksheet_name: The name of the worksheet to append to.
    """
    gc = get_google_sheets_client()
    spreadsheet = gc.open_by_key(spreadsheet_id)
    worksheet = spreadsheet.worksheet(worksheet_name)

    rows = df.values.tolist()
    worksheet.append_rows(rows)
    logger.info(f"Appended {len(rows)} rows to '{worksheet_name}'")