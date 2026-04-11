import logging

import awswrangler as wr
from airflow.sdk import Variable

from plugins.backfills_helper import get_backfill_dates
from plugins.date_utils import partitioned_date
from plugins.s3_helper import get_s3_client, write_partitioned_df

wr.engine.set("python")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


logging.info(f"Execution Engine: {wr.engine.get()}")
logging.info(f"Memory Format: {wr.memory_format.get()}")


def run_compaction(target_date: str):
    """
    Compacts raw JSON files for a given date into partitioned output data
    and deletes processed raw files.
    """

    s3 = get_s3_client()
    source_bucket = Variable.get("source_bucket")
    target_bucket = Variable.get("target_bucket")

    source_prefix = f"raw-ingestion/daily-stream/{target_date}/"
    source_path = f"s3://{source_bucket}/{source_prefix}"
    output_path = f"s3://{target_bucket}/compacted/"

    logging.info("Checking for files in: %s", source_path)

    objects = wr.s3.list_objects(source_path)

    if not objects:
        logging.info("No files found for %s. Skipping", target_date)
        return "No files found. Skipping compaction."

    logging.info("Reading JSON files from: %s", source_path)

    df = wr.s3.read_json(path=objects, use_threads=True)
    df = partitioned_date(target_date, df)

    if df.empty:
        logging.info("No data found after reading files. Nothing to compact")
        return "No data found. Nothing to compact."

    # Normalize values
    for col in df.columns:
        df[col] = df[col].fillna("").astype(str)

    logging.info("Writing compacted data to: %s", output_path)
    write_partitioned_df(df, output_path)

    logging.info("data written successfully. Rows written: %d", len(df))
    logging.info("Deleting processed raw files")

    for obj in objects:
        key = obj.replace(f"s3://{source_bucket}/", "")
        s3.delete_object(Bucket=source_bucket, Key=key)

    logging.info("Deleted %d source files.", len(objects))


def run_compaction_task():
    """ handles backfill and target dates."""
    for target_date in get_backfill_dates():
        run_compaction(target_date)
