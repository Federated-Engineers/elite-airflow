import logging

import awswrangler as wr
import boto3
import pandas as pd

logger = logging.getLogger(__name__)


def get_latest_s3_file(bucket: str, prefix: str):
    """
    A function that returns the path of the last modified data
    in an S3 bucket.

    Args:
        bucket: S3 bucket name.
        prefix: Prefix/Folder name to search in.
    Returns:
        The S3 path of the latest file.
    """

    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}/")
    files = response.get("Contents", [])

    if not files:
        raise ValueError(f"No files found in s3://{bucket}/{prefix}")

    logger.info(
        f"{len(files)} file(s) found in s3://{bucket}/{prefix}"
        )

    latest_object = max(files, key=lambda x: x["LastModified"])

    # last_modified = latest_object["LastModified"]
    # today = datetime.now(timezone.utc)
    # date_difference = (today.date() - last_modified.date()).days

    # if date_difference > last_date:
    #     logger.info("No new file to process today.")
    #     return None

    latest_file_path = f"s3://{bucket}/{latest_object['Key']}"
    logger.info(f"Latest file: {latest_file_path}")
    return latest_file_path


def write_df_to_s3(df, bucket_name, folder_name, file_name,
                   dataset=False, database=None, table=None):
    """
    Write a pandas DataFrame to S3 as a Parquet file.

    Args:
        df (pd.DataFrame): DataFrame to write.
        bucket_name (str): S3 bucket name.
        folder_name (str): Folder/prefix in S3.
        file_name (str): Name of the file (including .parquet).
        dataset (bool): Whether to write as a partitioned dataset.
        database (str | None): Glue/Athena catalog: Database name.
        table (str | None): Glue/Athena catalog: Table name.

    Returns:
        str: Full S3 path of the uploaded file.
    """
    s3_path = f"s3://{bucket_name}/{folder_name}/{file_name}"

    wr.s3.to_parquet(
        df=df,
        path=s3_path,
        dataset=dataset
    )

    return f"Data successfully written to {s3_path}"


def write_dataframe_to_s3_glue(
    df: pd.DataFrame,
    path: str,
    filename_prefix: str,
    partition_cols: list[str] = None,
    database: str | None = None,
    table: str | None = None,
    mode: str = "append",
) -> None:
    """
    Writes a pandas DataFrame to Amazon S3 in Parquet format and optionally
    registers/updates it in the AWS Glue Data Catalog.

    Args:
        df (pd.DataFrame):The DataFrame to be written to S3.
        path (str):The full S3 path where the dataset will be stored.
            Example: "s3://my-bucket/my-folder/"
        partition_cols (list[str]): List of column names to partition by
            Example: ["year", "month", "day"]
        filename_prefix (str):Prefix added to each output file name.
            Example: "2026-04-10_12:00:00_"
        database (str | None, optional): AWS Glue database name.
            If provided, the dataset will be registered in the Glue Catalog.
            If None, data will only be written to S3.
        table (str | None, optional): AWS Glue table name.
            Must be provided if `database` is specified.
        mode (str, optional): Write mode for the dataset:
                - "append": Adds new data to existing dataset
                - "overwrite_partitions": Replaces only affected partitions
                - "overwrite": Replaces entire dataset
            Default is "append".
    Returns:
        None
    """
    logger.info(f"Writing DataFrame to S3 at {path} with mode={mode}")

    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode=mode,
        partition_cols=partition_cols,
        database=database,
        table=table,
        filename_prefix=filename_prefix,
    )

    logger.info("Write to S3 completed successfully.")
