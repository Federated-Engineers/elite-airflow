CREATE TABLE IF NOT EXISTS {{ var.value.database }}.BRONZE.daily_extract_json (
    file_name          VARCHAR,
    file_row_number    NUMBER,
    file_last_modified TIMESTAMP_NTZ,
    loaded_at          TIMESTAMP_LTZ,
    json_data          VARIANT
);


COPY INTO {{ var.value.database }}.bronze.daily_extract_json
    (file_name, file_row_number, file_last_modified, loaded_at, json_data)
FROM (
    SELECT
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        METADATA$FILE_LAST_MODIFIED,
        CURRENT_TIMESTAMP(),
        $1
    FROM @{{ var.value.database }}..BRONZE.LONESTAR_EXTERNALS3_STAGE
)
FILE_FORMAT = (FORMAT_NAME = {{ var.value.database }}..BRONZE.LONESTAR_JSON_FILES);
