import logging
from datetime import date

import awswrangler as wr
import pandas as pd

from plugins.google_sheet import get_data_from_gsheet

logger = logging.getLogger(__name__)


def load_gsheet_to_s3(
        googlesheet_id: str,
        ssm_path: str,
        folder_path: str,
        file_name: str,
        partition_date: str = date.today().strftime('%Y-%m-%d'),
        run_id: str | None = None,
) -> str | None:
    """
    Function to load googlesheet data to aws bucket

    Args:
        googlesheet_id: The Google Sheet ID/key.
        ssm_path: The SSM parameter path of Google service account
            credentials
        folder_path: path to object folder in s3
        file_name: name of file being ingested
        partition_date: Date partition for the S3 object. Defaults to
            today's date when called outside Airflow.
        run_id: Optional unique run identifier used to distinguish
            multiple successful runs for the same partition date.

    Returns:
        The S3 object path if data was loaded, otherwise None.
    """

    output_file_name = (
        f"{file_name}_{run_id}.csv"
        if run_id
        else f"{file_name}.csv"
    )

    file_path = f"{folder_path}/date={partition_date}/{output_file_name}"
    data = get_data_from_gsheet(
        gsheet_id=googlesheet_id,
        ssm_path=ssm_path
    )
    if not data or len(data) == 0:
        logger.warning(f"No data found in Google Sheet {googlesheet_id}")
        return None
    else:
        try:
            dataframe = pd.DataFrame(data)
            wr.s3.to_csv(
                df=dataframe,
                path=file_path,
                dataset=False
                )

            logger.info(f"{len(dataframe)} records loaded to {file_path}")
            return file_path
        except Exception as e:
            logger.error(e)
            raise
