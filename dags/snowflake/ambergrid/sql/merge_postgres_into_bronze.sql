MERGE INTO {{ params.bronze_schema }}.{{ params.bronze_table }} AS target
USING (
    SELECT
        $1:record                   AS raw_record,
        $1:_snapshot_date::DATE     AS snapshot_date,
        METADATA$FILENAME           AS source_file_name,
        METADATA$FILE_ROW_NUMBER    AS file_row_number,
        METADATA$FILE_LAST_MODIFIED AS file_last_modified,
        METADATA$FILE_CONTENT_KEY   AS file_content_key
    FROM @{{ params.bronze_schema }}.{{ params.stage_name }}/{{ ti.xcom_pull(task_ids=params.snapshot_task_id) }}/
         (FILE_FORMAT => '{{ params.bronze_schema }}.POSTGRES_JSON_FORMAT')
) AS source
ON target.RAW_RECORD:{{ params.row_key }}::VARCHAR
    = source.raw_record:{{ params.row_key }}::VARCHAR

WHEN MATCHED AND TO_JSON(target.RAW_RECORD) != TO_JSON(source.raw_record)
    THEN UPDATE SET
        RAW_RECORD         = source.raw_record,
        SNAPSHOT_DATE      = source.snapshot_date,
        SOURCE_FILE_NAME   = source.source_file_name,
        FILE_ROW_NUMBER    = source.file_row_number,
        FILE_LAST_MODIFIED = source.file_last_modified,
        FILE_CONTENT_KEY   = source.file_content_key,
        LOADED_AT          = CURRENT_TIMESTAMP()

WHEN MATCHED
    THEN UPDATE SET
        SNAPSHOT_DATE = source.snapshot_date

WHEN NOT MATCHED
    THEN INSERT (
        RAW_RECORD,
        SOURCE_TABLE,
        SNAPSHOT_DATE,
        SOURCE_FILE_NAME,
        FILE_ROW_NUMBER,
        FILE_LAST_MODIFIED,
        FILE_CONTENT_KEY,
        LOADED_AT
    )
    VALUES (
        source.raw_record,
        '{{ params.source_table }}',
        source.snapshot_date,
        source.source_file_name,
        source.file_row_number,
        source.file_last_modified,
        source.file_content_key,
        CURRENT_TIMESTAMP()
    );
