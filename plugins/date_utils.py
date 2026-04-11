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
    current_datetime = datetime.datetime.now().strftime("%d-%m-%Y_%H:%M:%S")
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


def partitioned_date(target_date, df=None):
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
