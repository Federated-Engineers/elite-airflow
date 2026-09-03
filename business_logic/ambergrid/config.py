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

# Postgres source
POSTGRES_CONN_ID = "ambergrid_postgres"
POSTGRES_SCHEMA = "ambergrid"
PG_STAGE = "POSTGRES_AMBERGRID_STAGE"

POSTGRES_TABLE_TO_BRONZE_TABLE = {
    "plants": (
        "PG_PLANTS", "plant_id"
    ),
    "suppliers": (
        "PG_SUPPLIERS", "supplier_id"
    ),
    "supplier_contracts": (
        "PG_SUPPLIER_CONTRACTS", "contract_id"
    ),
    "fertilizer_sales_invoices": (
        "PG_FERTILIZER_SALES_INVOICES", "invoice_id"
    ),
    "fleet_logistics_logs": (
        "PG_FLEET_LOGISTICS_LOGS", "pickup_id"
    ),
    "gas_grid_injection_daily": (
        "PG_GAS_GRID_INJECTION_DAILY", "record_id"
    ),
}

# External table
SCADA_EXTERNAL_TABLE = "SCADA_TELEMETRY"
