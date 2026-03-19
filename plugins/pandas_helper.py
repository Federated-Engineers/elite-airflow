import pandas as pd


def pandas_json_normalizer(json_list: list):
    """
    This normalized list of json to a pandas dataframe.

    Args:
        json_list (dict): List of jsons.

    Returns:
        pd.DataFrame
    """
    return pd.json_normalize(json_list)
