ALTER EXTERNAL TABLE {{ params.bronze_schema }}.{{ params.external_table }}
    REFRESH;
