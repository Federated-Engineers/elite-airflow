import pandas as pd


def pandas_json_normalizer(json_list: list):
    """
    This normalized list of json to a pandas dataframe.

    Args:
        json_list (dict): List of jsons.

    Returns:
        pd.DataFrame
    """
    return pd.json_normalize(json_list)


def write_partitioned_df(df, path, dataset=True):
    """
    Write a DataFrame to S3 as a partitioned Parquet dataset.
    Partitions by year, month, and day using overwrite_partitions mode,
    ensure the DataFrame has no duplicate partitions before writing.
    """
    if df is None:
        raise ValueError("df cannot be None")
    if path is None:
        raise ValueError("path cannot be None")

    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=dataset,
        partition_cols=["year", "month", "day"],
        mode="overwrite_partitions"
    )
