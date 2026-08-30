import json
import logging
import os
import tempfile

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from business_logic.ambergrid import config

logger = logging.getLogger(__name__)

STAGE_NAME = f"{config.BRONZE_SCHEMA}.{config.PG_STAGE}"


def _read_source_table(source_table: str) -> list:
    """Read every row of one source table.

    Args:
        source_table: Table name within the ambergrid schema.
    Returns:
        A list of dicts, one per row, keyed by column name.
    """

    hook = PostgresHook(postgres_conn_id=config.POSTGRES_CONN_ID)

    with hook.get_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT * FROM {config.POSTGRES_SCHEMA}.{source_table}"
            )
            column_names = [column[0] for column in cursor.description]
            rows = cursor.fetchall()

    logger.info(f"Read {len(rows)} rows from '{source_table}'")

    return [dict(zip(column_names, row)) for row in rows]


def snapshot_table_to_stage(source_table: str, snapshot_date: str) -> str:
    """Snapshot one Postgres table into the Snowflake internal stage.

    Args:
        source_table: Table name within the ambergrid schema.
        snapshot_date: Capture date as YYYY-MM-DD.
    Returns:
        The stage path, for the MERGE task to read.
    """

    stage_path = f"{source_table}/snapshot_date={snapshot_date}"
    hook = SnowflakeHook(snowflake_conn_id=config.SNOWFLAKE_CONN_ID)

    database = hook.get_first("SELECT CURRENT_DATABASE()")[0]
    logger.info(f"Target database: {database}")

    if hook.get_records(f"LIST @{STAGE_NAME}/{stage_path}/"):
        logger.info(f"Snapshot already staged at {stage_path}")
        return stage_path

    rows = _read_source_table(source_table)

    if not rows:
        raise ValueError(f"Table '{source_table}' returned no rows.")

    records = [
        {"_snapshot_date": snapshot_date, "record": row}
        for row in rows
    ]

    with tempfile.TemporaryDirectory() as temp_dir:
        local_file_path = os.path.join(temp_dir, f"{source_table}.json")

        with open(local_file_path, "w", encoding="utf-8") as json_file:

            json.dump(
                records,
                json_file,
                ensure_ascii=False,
                indent=2,
                default=str,
            )

        hook.run(
            f"PUT file://{local_file_path} "
            f"@{STAGE_NAME}/{stage_path}/ "
            f"AUTO_COMPRESS = TRUE OVERWRITE = FALSE"
        )

    logger.info(f"Staged {len(records)} rows at {stage_path}")

    return stage_path
