from datetime import datetime, timezone

import requests
from airflow.models import Variable

from plugins.date_time import get_next_day_utc
from plugins.pandas_helper import pandas_json_normalizer
from plugins.s3_helper import write_df_to_s3


def fetch_weatherapi_data(**context):
    """
    Fetches WeatherAPI data for Mallorca and writes to S3.

    Behavior:
    - If Airflow Variable 'bbss_backfill_date' exists, fetch history.json for that date.
    - If not, fetch forecast.json for the next day relative to execution_date.
    
    Fully backfill-safe and config-driven.
    """
    config = Variable.get("BBSS_CONFIG", deserialize_json=True)
    weather_config = config["weatherapi"]

    api_key = weather_config["api_key"]
    bucket_name = weather_config["bucket_name"]
    folder_name = weather_config["folder_name"]
    location = weather_config["location"]

    backfill_date = Variable.get("bbss_backfill_date", default_var=None)

    if backfill_date:
        url = "https://api.weatherapi.com/v1/history.json"
        params = {
            "key": api_key,
            "q": location,
            "dt": backfill_date
        }
        print(f"Running backfill for date: {backfill_date}")

    else:
        execution_date = context["execution_date"]
        next_day_date = get_next_day_utc(execution_date)
        forecast_date = next_day_date.strftime("%Y-%m-%d")

        url = "https://api.weatherapi.com/v1/forecast.json"
        params = {
            "key": api_key,
            "q": location,
            "dt": forecast_date
        }
        print(f"Running forecast for date: {forecast_date}")

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    forecast_response_data = response.json()

    df = pandas_json_normalizer(forecast_response_data)

    ingestion_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_name = f"{ingestion_timestamp}.parquet"

    return write_df_to_s3(
        df=df,
        bucket_name=bucket_name,
        folder_name=folder_name,
        file_name=file_name,
        dataset=False
    )
