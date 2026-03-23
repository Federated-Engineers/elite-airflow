import datetime
import logging


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
