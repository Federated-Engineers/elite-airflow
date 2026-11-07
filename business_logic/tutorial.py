import logging

import pandas as pd

logger = logging.getLogger(__name__)


# Sample orders for each region. In a real pipeline this would come
# from a database, an API or a file sitting in S3.
ORDERS = {
    "north": [
        {"product": "rice", "quantity": 10, "unit_price": 45.0},
        {"product": "beans", "quantity": 4, "unit_price": 30.0},
        {"product": "oil", "quantity": 6, "unit_price": 25.0},
    ],
    "south": [
        {"product": "rice", "quantity": 7, "unit_price": 45.0},
        {"product": "sugar", "quantity": 12, "unit_price": 15.0},
    ],
    "east": [
        {"product": "beans", "quantity": 9, "unit_price": 30.0},
        {"product": "oil", "quantity": 3, "unit_price": 25.0},
        {"product": "sugar", "quantity": 5, "unit_price": 15.0},
    ],
    "west": [
        {"product": "rice", "quantity": 15, "unit_price": 45.0},
        {"product": "oil", "quantity": 8, "unit_price": 25.0},
    ],
}


def get_region_sales(region):
    """
    Work out the total sales for one region.

    Whatever this function returns is saved to XCom, so the
    combine task can read it later.
    """
    df = pd.DataFrame(ORDERS[region])
    df["amount"] = df["quantity"] * df["unit_price"]

    total_sales = float(df["amount"].sum())
    logger.info(f"{region}: {len(df)} orders, total sales = {total_sales}")

    return total_sales


def combine_region_sales(**context):
    """
    Read the four region totals from XCom and add them up.
    """
    ti = context["ti"]

    region_totals = {}
    for region in ORDERS:
        region_totals[region] = ti.xcom_pull(task_ids=f"get_{region}_sales")

    for region, total in region_totals.items():
        logger.info(f"{region}: {total}")

    overall_total = sum(region_totals.values())
    logger.info(f"Overall sales across all regions = {overall_total}")

    return overall_total
