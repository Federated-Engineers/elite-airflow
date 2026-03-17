from datetime import datetime, timezone

import requests
from airflow.sdk import Variable

from plugins.pandas_helper import normalize_weather_forecast
from plugins.date_time import get_next_day_utc
from plugins.s3_helper import write_df_to_s3


def fetch_weatherapi_data(**context):
    """
    Fetches WeatherAPI forecast data for Mallorca and writes to S3.
    Fully backfill-safe and config-driven.
    """
    execution_date = context.get("execution_date")

    config = Variable.get("BBS_CONFIG", deserialize_json=True)
    weather_config = config["weatherapi"]

    api_key = weather_config["api_key"]
    bucket_name = weather_config["bucket_name"]
    folder_name = weather_config["folder_name"]
    location = weather_config["location"]

    next_day_date = get_next_day_utc(execution_date)
    forecast_date = next_day_date.strftime("%Y-%m-%d")

    url = "https://api.weatherapi.com/v1/forecast.json"
    params = {
        "key": api_key,
        "q": location,
        "dt": forecast_date
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    forecast_response_data = response.json()

    df = normalize_weather_forecast(forecast_response_data)

    ingestion_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_name = f"{ingestion_timestamp}.parquet"

    return write_df_to_s3(
        df=df,
        bucket_name=bucket_name,
        folder_name=folder_name,
        file_name=file_name,
        dataset=False
    )