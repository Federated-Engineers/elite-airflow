import pandas as pd


def normalize_weather_forecast(json_data):
    """
    Normalizes hourly forecast data from WeatherAPI JSON response
    into a pandas DataFrame.

    Args:
        json_data (dict): Raw JSON response from WeatherAPI.

    Returns:
        pd.DataFrame: Normalized hourly forecast data.
    """
    return pd.json_normalize(
        json_data["forecast"]["forecastday"][0]["hour"]
    )
