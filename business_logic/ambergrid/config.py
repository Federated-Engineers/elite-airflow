SNOWFLAKE_CONN_ID = "snowflake_ambergrid"
BRONZE_SCHEMA = "BRONZE"

LAB_LEDGER_STAGE = "GSHEET_LAB_LEDGER_STAGE"

WORKSHEET_TO_BRONZE_TABLE = {
    "NPK_Lab_Batches": (
        "GSHEET_NPK_LAB_BATCHES", "batch_id"
    ),
    "Subsidy_Grants": (
        "GSHEET_SUBSIDY_GRANTS", "grant_id"
    ),
    "Fertilizer_Price_Adjustments": (
        "GSHEET_FERTILIZER_PRICE_ADJUSTMENTS", "adjustment_id"
    ),
    "Impurity_WriteOffs": (
        "GSHEET_IMPURITY_WRITEOFFS", "writeoff_id"
    ),
}
