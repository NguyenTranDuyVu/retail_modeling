-- Step 1: Update existing active rows to inactive (no WHERE clause on merge)
MERGE INTO SNOWFLAKE_LEARNING_DB.L2.TRANSACTION_DIM target
USING SNOWFLAKE_LEARNING_DB.L1.TRANSACTION_STG source
ON target.payment_key = source.payment_key
AND target.is_current = TRUE
WHEN MATCHED THEN
    UPDATE SET
        is_current = FALSE,
        effective_end_date = CURRENT_DATE;

-- Step 2: Insert new active rows from staging
INSERT INTO SNOWFLAKE_LEARNING_DB.L2.TRANSACTION_DIM (
    payment_key,
    trans_type,
    bank_name,
    effective_start_date,
    effective_end_date,
    is_current,
    ingest_time
)
SELECT
    payment_key,
    trans_type,
    bank_name,
    CURRENT_DATE,
    NULL,
    TRUE,
    ingest_time
FROM SNOWFLAKE_LEARNING_DB.L1.TRANSACTION_STG;
