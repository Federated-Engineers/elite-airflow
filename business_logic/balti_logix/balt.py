import logging

import awswrangler as wr
from airflow.sdk import Variable

from plugins.process_dates import run_dt
from plugins.date_utils import get_partitioned_date
from plugins.aws import get_s3_client
from plugins.pandas_helper import write_partitioned_df

wr.engine.set("python")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def run_compaction(target_date: str):
    """
    Compacts raw JSON files for a given date into partitioned output data
    and deletes processed raw files.
    """

    s3 = get_s3_client()
    balti_s3 = Variable.get("balti_s3", deserialize_json=True)
    source_bucket = balti_s3["source_bucket"]
    target_bucket = balti_s3["target_bucket"]

    source_prefix = f"raw-ingestion/daily-stream/{target_date}/"
    source_path = f"s3://{source_bucket}/{source_prefix}"
    output_path = f"s3://{target_bucket}/compacted/"

    logging.info(f"Checking for files in: {source_path}")

    objects = wr.s3.list_objects(source_path)

    if not objects:
        logging.info(f"No files found for {target_date}. Skipping")
        return "No files found. Skipping compaction."

    logging.info(f"Reading JSON files from: {source_path}")

    df = wr.s3.read_json(path=objects)
    df = get_partitioned_date(target_date, df)

    if df.empty:
        return "No data found. Nothing to compact."

    # Normalize values
    for col in df.columns:
        df[col] = df[col].fillna("").astype(str)

    logging.info(f"Writing compacted data to: {output_path}")
    write_partitioned_df(df, output_path)

    logging.info("data written successfully. Rows written: %d", len(df))
    logging.info("Deleting processed raw files")

    for obj in objects:
        key = obj.replace(f"s3://{source_bucket}/", "")
        s3.delete_object(Bucket=source_bucket, Key=key)

    logging.info("Deleted %d source files.", len(objects))


def run_compaction_task():
    """ handles backfill and target dates."""
    for target_date in run_dt():
        run_compaction(target_date)
