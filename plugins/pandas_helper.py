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


def convert_columns_to_datetime(df: pd.DataFrame, columns: list[str]):
    """
    Converts specified columns in a pandas DataFrame to datetime.
    Invalid values are coerced toNaT(special datetime null value)
    Args:
        df (pd.DataFrame): Input dataframe.
        columns (list[str]): Columns to convert to datetime.

    Returns:
        pd.DataFrame: Updated dataframe with datetime columns.
    """
    for col in columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def add_ingestion_timestamp(df: pd.DataFrame):
    """
    Adds ingestion timestamp column (UTC, timezone-aware).
    """
    df["ingestion_timestamp"] = pd.Timestamp.now(tz="UTC")
    return df
