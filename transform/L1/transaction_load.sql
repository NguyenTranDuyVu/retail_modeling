
COPY INTO SNOWFLAKE_LEARNING_DB.L1.TRANSACTION_STG (
    payment_key,
    trans_type,
    bank_name,
    ingest_time
)
FROM (
    SELECT
        $1,
        $2,
        $3,
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