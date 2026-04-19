import logging

import awswrangler as wr
import pandas as pd

from plugins.date_utils import get_current_datetime
from plugins.google_sheet import get_data_from_gsheet
from plugins.pandas_helper import convert_columns_to_datetime
from plugins.s3_helper import write_dataframe_to_s3_glue

logger = logging.getLogger(__name__)


def gsheet_to_s3_dataset(
    gsheet_id: str,
    ssm_path: str,
    bucket: str,
    path_dir: str,
    database: str,
    table_name: str,
    date_column: str,
):

    data = get_data_from_gsheet(gsheet_id, ssm_path)
    df = pd.DataFrame(data)

    logger.info(f"Fetched data from Google Sheets | rows={len(df)}")

    df = convert_columns_to_datetime(df, [date_column])

    df["year"] = df[date_column].dt.year
    df["month"] = df[date_column].dt.month
    df["day"] = df[date_column].dt.day

    wr.engine.set("python")

    path = f"s3://{bucket}/{path_dir}"
    now = get_current_datetime()

    partitions = df[["year", "month", "day"]].drop_duplicates()

    logger.info(f"Total partitions to process: {len(partitions)}")

    for _, row in partitions.iterrows():
        year, month, day = row["year"], row["month"], row["day"]

        logger.info(
            f"Processing partition: year={year}, "
            f"month={month}, day={day}"
        )

        partition_path = f"{path}/year={year}/month={month}/day={day}/"

        df_partition = df[
            (df["year"] == year) &
            (df["month"] == month) &
            (df["day"] == day)
        ]

        logger.info(
            f"Partition rows (new): {len(df_partition)}"
        )

        try:
            existing_df = wr.s3.read_parquet(path=partition_path)
            logger.info(f"Existing partition rows: {len(existing_df)}")
        except Exception as e:
            logger.warning(
                f"No existing data for partition: {str(e)}"
            )
            existing_df = pd.DataFrame()

        if not existing_df.empty:
            common_cols = sorted(
                set(df_partition.columns)
                & set(existing_df.columns)
            )

            df_aligned = df_partition[common_cols]
            existing_aligned = existing_df[common_cols]

            df_sorted = (
                df_aligned
                .sort_values(by=common_cols)
                .reset_index(drop=True)
            )
            existing_sorted = (
                existing_aligned
                .sort_values(by=common_cols)
                .reset_index(drop=True)
            )

            try:
                pd.testing.assert_frame_equal(
                    df_sorted,
                    existing_sorted,
                    check_dtype=False,
                    check_like=True
                )

                logger.info(
                    f"No changes detected → Skipping partition "
                    f"year={year}/month={month}/day={day}"
                )
                continue

            except AssertionError:
                logger.warning(
                    f"Data mismatch → Overwriting partition "
                    f"year={year}/month={month}/day={day}"
                )

        else:
            logger.info("First load for this partition → writing data")

        write_dataframe_to_s3_glue(
            df=df_partition,
            path=path,
            partition_cols=["year", "month", "day"],
            database=database,
            table=table_name,
            filename_prefix=f"{now}_",
            mode="overwrite_partitions",
        )

        logger.info(
            f"Partition written | rows={len(df_partition)} | "
            f"year={year}/month={month}/day={day}"
        )
