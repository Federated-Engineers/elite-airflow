from datetime import datetime, timedelta, timezone

def get_next_day_utc(execution_date: datetime = None):
    """
    Returns the next day's date in UTC, optionally relative to execution_date.

    Args:
        execution_date (datetime, optional): Reference datetime.
        Defaults to now UTC.

    Returns:
        datetime.date: Next day's date in UTC.
    """
    if execution_date is None:
        execution_date = datetime.now(timezone.utc)
    return execution_date.date() + timedelta(days=1)



