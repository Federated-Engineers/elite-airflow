COPY INTO {{ params.bronze_schema }}.{{ params.bronze_table }} (
    RAW_RECORD,
    SHEET_ROW_NUMBER,
    WORKSHEET_NAME,
    SNAPSHOT_DATE,
    SOURCE_FILE_NAME,
    FILE_ROW_NUMBER,
    FILE_LAST_MODIFIED,
    FILE_CONTENT_KEY,
    LOADED_AT
)
FROM (
    SELECT
        $1:record,
        $1:_sheet_row_number::NUMBER,
        '{{ params.worksheet }}',
        $1:_snapshot_date::DATE,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        METADATA$FILE_LAST_MODIFIED,
        METADATA$FILE_CONTENT_KEY,
        CURRENT_TIMESTAMP()
    FROM @{{ params.bronze_schema }}.{{ params.stage_name }}/{{ ti.xcom_pull(task_ids=params.snapshot_task_id) }}/
)
FILE_FORMAT = (
    FORMAT_NAME = '{{ params.bronze_schema }}.GSHEET_JSON_ARRAY_FORMAT'
)
ON_ERROR = 'ABORT_STATEMENT';
