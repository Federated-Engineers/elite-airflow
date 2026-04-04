import logging
from datetime import datetime, timedelta, timezone


def get_current_datetime():
    """
    A function that returns the current date and time of ingestion
    Args:
        None
    Returns
        string_datetime
    """
    logging.info("Getting the current date and time of ingestion")
    current_datetime = datetime.now().strftime("%d-%m-%Y_%H:%M:%S")
    return current_datetime


def get_next_day_utc():
    """
    Returns the next day's date in UTC.

    Returns:
        datetime.date: Next day's date in UTC.
    """
    return datetime.now(timezone.utc).date() + timedelta(days=1)
