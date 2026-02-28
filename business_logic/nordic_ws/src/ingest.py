import pandas as pd
from business_logic.nordic_ws.src.s3 import read_parquet, write_parquet


def normalize_dates(df, date_col):
    """Ensure datetime column is timezone-naive."""
    if date_col and date_col in df.columns:
        df[date_col] = (
            pd.to_datetime(df[date_col], utc=True)
              .dt.tz_convert(None)
              .astype("datetime64[us]")  # enforce consistent resolution
        )
    return df

def ingest_sheet(gspread_client, sheet_config):
    """Pull sheet, deduplicate against existing S3 data, append new rows."""
    name = sheet_config["name"]
    unique_keys = sheet_config["unique_keys"]
    print(f"\n[{name}] Starting ingestion...")

    # 1. Pull from Google Sheets
    sheet = gspread_client.open_by_key(sheet_config["id"])
    df_new = pd.DataFrame(sheet.get_worksheet(0).get_all_records())

    if df_new.empty:
        print(f"[{name}] Sheet is empty, skipping.")
        return

    date_col = (
        "date" if "date" in df_new.columns
        else "signup_date" if "signup_date" in df_new.columns
        else None
    )
    
    df_new = normalize_dates(df_new, date_col)

    # 2. Load existing data from S3
    s3_key = f"{sheet_config['s3_folder']}/latest.parquet"
    df_existing = read_parquet(s3_key)

    # 3. Incremental merge — only append new date+product_id combos
    if df_existing.empty:
        df_final = df_new
        print(f"[{name}] First load — {len(df_final)} rows")
    else:
        df_existing = normalize_dates(df_existing, date_col)
        merged = df_new.merge(
            df_existing[unique_keys],
            on=unique_keys,
            how="left",
            indicator=True
        )
        df_delta = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
        df_final = pd.concat([df_existing, df_delta], ignore_index=True, join="outer")\
                     .sort_values(unique_keys)\
                     .reset_index(drop=True)
        print(f"[{name}] Appended {len(df_delta)} new rows (total: {len(df_final)})")

    # 4. Write back to S3
    write_parquet(df_final, s3_key)