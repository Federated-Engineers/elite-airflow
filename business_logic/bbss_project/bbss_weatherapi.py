import awswrangler as wr
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from airflow.sdk import Variable


def fetch_weatherapi_data():
    api_key = Variable.get("WEATHERAPI_KEY").strip()

    next_day_str = (
        datetime.now(timezone.utc).date() + timedelta(days=1)
    ).strftime("%Y-%m-%d")

    params = {
        "key": api_key,
        "q": "Mallorca",
        "dt": next_day_str
    }

    url = "https://api.weatherapi.com/v1/forecast.json"
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    json_data = response.json()

    df = pd.json_normalize(json_data["forecast"]["forecastday"][0]["hour"])
    df["ingested_at"] = datetime.now(timezone.utc)

    bucket_name = "federated-engineers-production-elite-balearic"
    folder_name = "weather_data"

    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    unique_file = f"{timestamp_str}.parquet"

    s3_path = f"s3://{bucket_name}/{folder_name}/{unique_file}"

    wr.s3.to_parquet(
     df=df,
     path=s3_path,
     dataset=False
    )
