import logging
from datetime import datetime, timedelta, timezone

from airflow.sdk import Variable

from plugins.date_utils import get_yesterday


def run_dt():
    """
    Returns a list of target dates for compaction.
    If backfill Variable is set, returns all dates in range.
    Otherwise returns yesterday's date as a single item list.

    Returns:
        list: List of date strings in format 'YYYY-MM-DD'

    Raises:
        ValueError: If date range is invalid or end date is not in the past.
    """
    backfill = Variable.get("backfill_dates", deserialize_json=True)

    if backfill:
        start = datetime.strptime(backfill["start"], "%Y-%m-%d").date()
        end = datetime.strptime(backfill["end"], "%Y-%m-%d").date()

        if start > end:
            raise ValueError(f"start '{start}' must be before end '{end}'.")
        if end >= datetime.now(timezone.utc).date():
            raise ValueError(f"end date '{end}' must be in the past.")

        current = start
        target_dates = []
        while current <= end:
            target_dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)

        logging.info(
            "Running backfill for {len(target_dates)} dates: {start} to {end}"
            )
        return target_dates

    target_date = get_yesterday()
    logging.info("No backfill set. Running for yesterday: %s", target_date)
    return [target_date]
