import pandas as pd
from nordic_ws.src.s3 import read_parquet, write_parquet

def ingest_sheet(gspread_client, sheet_config):
    """Pull sheet, deduplicate against existing S3 data, append new rows."""
    name = sheet_config["name"]
    print(f"\n[{name}] Starting ingestion...")

    # 1. Pull from Google Sheets
    sheet = gspread_client.open_by_key(sheet_config["id"])
    df_new = pd.DataFrame(sheet.get_worksheet(0).get_all_records())

    if df_new.empty:
        print(f"[{name}] Sheet is empty, skipping.")
        return

    date_col = "date" if "date" in df_new.columns else "signup_date" if "signup_date" in df_new.columns else None
    if date_col:
        df_new[date_col] = pd.to_datetime(df_new[date_col])

    # 2. Load existing data from S3
    s3_key = f"{sheet_config['s3_folder']}/latest.parquet"
    df_existing = read_parquet(s3_key)

    # 3. Incremental merge — only append new date+product_id combos
    if df_existing.empty:
        df_final = df_new
        print(f"[{name}] First load — {len(df_final)} rows")
    else:
        df_existing["date"] = pd.to_datetime(df_existing["date"])
        merged = df_new.merge(
            df_existing[UNIQUE_KEYS],
            on=UNIQUE_KEYS,
            how="left",
            indicator=True
        )
        df_delta = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
        df_final = pd.concat([df_existing, df_delta], ignore_index=True)\
                     .sort_values(UNIQUE_KEYS)\
                     .reset_index(drop=True)
        print(f"[{name}] Appended {len(df_delta)} new rows (total: {len(df_final)})")

    # 4. Write back to S3
    write_parquet(df_final, s3_key)