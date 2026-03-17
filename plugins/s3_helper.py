import awswrangler as wr


def write_df_to_s3(df, bucket_name, folder_name, file_name, dataset=False):
    """
    Write a pandas DataFrame to S3 as a Parquet file.

    Args:
        df (pd.DataFrame): DataFrame to write.
        bucket_name (str): S3 bucket name.
        folder_name (str): Folder/prefix in S3.
        file_name (str): Name of the file (including .parquet).
        dataset (bool): Whether to write as a partitioned dataset.

    Returns:
        str: Full S3 path of the uploaded file.
    """
    s3_path = f"s3://{bucket_name}/{folder_name}/{file_name}"

    wr.s3.to_parquet(
        df=df,
        path=s3_path,
        dataset=dataset
    )

    return s3_path
