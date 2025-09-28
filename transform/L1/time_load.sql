
COPY INTO SNOWFLAKE_LEARNING_DB.L1.TIME_STG (
    time_key,
    date,
    hour,
    day,
    week,
    month,
    quarter,
    year,
    ingest_time
)
FROM (
    SELECT
        $1,
        TO_TIMESTAMP($2, 'DD-MM-YYYY HH24:MI'),
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        CURRENT_TIMESTAMP()
    FROM '@{{params.STAGE_NAME}}/{{params.STAGE_FILE_NAME}}'
)
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1
)
ON_ERROR = 'CONTINUE'
FORCE = TRUE;
