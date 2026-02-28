from business_logic.nordic_ws.config.sheets import SHEETS
from business_logic.nordic_ws.src.auth2 import get_gspread_client
from business_logic.nordic_ws.src.ingest import ingest_sheet


def run():
    print("=== Nordic Peaks Ingestion ===")

    # Auth once, reuse across all sheets
    gspread_client = get_gspread_client()

    for sheet in SHEETS:
        try:
            ingest_sheet(gspread_client, sheet)
        except Exception as e:
            print(f"[{sheet['name']}] FAILED: {e}")
            continue

    print("\n=== Done ===")


if __name__ == "__main__":
    run()
