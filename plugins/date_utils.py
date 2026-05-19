import logging
from datetime import datetime, timedelta, timezone

import pandas as pd


def get_current_datetime():
    """
    A function that returns the current date and time of ingestion
    Args:
        None
    Returns
        string_datetime
    """
    logging.info("Getting the current date and time of ingestion")
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    return current_datetime


def get_next_day_utc():
    """
    Returns the next day's date in UTC.
    Returns:
        datetime.date: Next day's date in UTC.
    """
    return datetime.now(timezone.utc).date() + timedelta(days=1)


def get_yesterday():
    """Returns yesterday's date as a string"""
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")


def get_partitioned_date(target_date, df):
    """
    Add partition columns (year, month, day) to a
    DataFrame based on a target date

    Returns:
        pandas.DataFrame: DataFrame with added
        `year`, `month`, and `day` columns.
    """
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    year, month, day = dt.year, dt.month, dt.day
    if df is None:
        df = pd.DataFrame()
    df["year"] = year
    df["month"] = month
    df["day"] = day
    return df


def date_partition_path(current_datetime) -> str:
    """
    A function that generates date hive partition path 
    based on the current date and time.
    Args:
        current_datetime (str): The current date and time.
        The expected format of current_datetime is "YYYY-MM-DD_HH:MM:SS".
        The get_current_datetime function can be used to generate the current_datetime 
        string in the correct format.
    Returns:
        str: A string containing a year/month/day partition path.
    """
    year, month, day = current_datetime.split("_")[0].split("-")

    date_hive_partition = f"year={year}/month={month}/day={day}"
    return date_hive_partition



