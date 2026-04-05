import json
import logging
from datetime import datetime

import awswrangler as wr
import boto3


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

wr.config.use_ray = False
wr.config.use_threads = True


s3 = boto3.client("s3")


def run_compaction(target_date):
    source_bucket = "federated-engineers-staging-elite-data-lake"
    target_bucket = "federated-engineers-staging-elite-data-lake"

    source_prefix = f"raw-ingestion/daily-stream/{target_date}/"
    source_path = f"s3://{source_bucket}/{source_prefix}"

    dt = datetime.strptime(target_date, "%Y-%m-%d")
    year, month, day = dt.year, dt.month, dt.day

    logging.info(f"Checking for files in: {source_path}")
    objects = wr.s3.list_objects(source_path)

    if not objects:
        logging.warning("No files found. Skipping compaction.")
        return {"status": "NO_DATA"}

    logging.info(f"Reading JSON files from: {source_path}")

    df = wr.s3.read_json(path=source_path, use_threads=True)

    if df.empty:
        logging.warning("No data found. Nothing to compact.")
        return {"status": "NO_DATA"}

    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].apply(
                lambda x: json.dumps(x) if isinstance(x, (dict, list))
                else str(x) if x is not None else None
            )

    # Add partition columns
    df["year"] = year
    df["month"] = month
    df["day"] = day

    output_path = f"s3://{target_bucket}/compacted/"

    logging.info(f"Writing compacted data to: {output_path}")

    wr.s3.to_parquet(
        df=df,
        path=output_path,
        dataset=True,
        partition_cols=["year", "month", "day"]
    )

    logging.info("Compacted data written successfully.")

    #  Delete processed raw files
    logging.info("Deleting processed raw files")

    objects = wr.s3.list_objects(source_path)

    if objects:
        for obj in objects:
            key = obj.replace(f"s3://{source_bucket}/", "")
            s3.delete_object(Bucket=source_bucket, Key=key)

        logging.info(f"Deleted {len(objects)} source files.")
    else:
        logging.warning("No source files found to delete.")

    return {
        "status": "SUCCESS",
        "rows_written": len(df),
        "partitions": {"year": year, "month": month, "day": day}
    }

