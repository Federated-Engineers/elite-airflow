import json
import logging
import os
import tempfile

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Variable

from business_logic.ambergrid import config
from plugins.google_sheet import get_values_from_gsheet

logger = logging.getLogger(__name__)

STAGE_NAME = f"{config.BRONZE_SCHEMA}.{config.LAB_LEDGER_STAGE}"


def _build_snapshot_records(
    sheet_values: list,
    column_names: list,
    snapshot_date: str,
) -> list:
    """Build one snapshot record per data row.

    Args:
        sheet_values: Every cell of the worksheet, row 0 being the header.
        column_names: The stripped header row.
        snapshot_date: Capture date as YYYY-MM-DD.
    Returns:
        A list of dicts, one per data row.
    """

    records = []

    for row_index, row in enumerate(sheet_values[1:]):

        padded_row = list(row) + [""] * (len(column_names) - len(row))

        records.append({
            "_snapshot_date": snapshot_date,
            "_sheet_row_number": row_index + 2,
            "record": dict(zip(column_names, padded_row)),
        })

    return records


def snapshot_worksheet_to_stage(worksheet: str, snapshot_date: str) -> str:
    """Snapshot one worksheet into the Snowflake internal stage.

    Args:
        worksheet: The worksheet (tab) name to read.
        snapshot_date: Capture date as YYYY-MM-DD.
    Returns:
        The stage path, for the COPY INTO task to load.
    """

    ambergrid_config = Variable.get("ambergrid_config", deserialize_json=True)
    stage_path = f"{worksheet}/snapshot_date={snapshot_date}"
    hook = SnowflakeHook(snowflake_conn_id=config.SNOWFLAKE_CONN_ID)

    database = hook.get_first("SELECT CURRENT_DATABASE()")[0]
    logger.info(f"Target database: {database}")

    if hook.get_records(f"LIST @{STAGE_NAME}/{stage_path}/"):
        logger.info(f"Snapshot already staged at {stage_path}")
        return stage_path

    sheet_values = get_values_from_gsheet(
        gsheet_id=ambergrid_config["lab_ledger_sheet_id"],
        ssm_path=ambergrid_config["google_ssm_path"],
        sheet_name=worksheet,
    )

    if not sheet_values:
        raise ValueError(f"Worksheet '{worksheet}' returned no rows.")

    column_names = [name.strip() for name in sheet_values[0]]
    logger.info(f"Columns for '{worksheet}': {column_names}")

    records = _build_snapshot_records(
        sheet_values, column_names, snapshot_date
    )

    if not records:
        raise ValueError(f"Worksheet '{worksheet}' has no data rows.")

    with tempfile.TemporaryDirectory() as temp_dir:
        local_file_path = os.path.join(temp_dir, f"{worksheet}.json")

        with open(local_file_path, "w", encoding="utf-8") as json_file:
            json.dump(records, json_file, ensure_ascii=False, indent=2)

        hook.run(
            f"PUT file://{local_file_path} "
            f"@{STAGE_NAME}/{stage_path}/ "
            f"AUTO_COMPRESS = TRUE OVERWRITE = FALSE"
        )

    logger.info(f"Staged {len(records)} rows at {stage_path}")

    return stage_path
