-- Step 1: Update existing active rows to inactive
MERGE INTO SNOWFLAKE_LEARNING_DB.L2.ITEM_DIM target
USING SNOWFLAKE_LEARNING_DB.L1.ITEM_STG source
ON target.item_key = source.item_key
   AND target.is_current = TRUE
WHEN MATCHED THEN
    UPDATE SET
        is_current = FALSE,
        effective_end_date = CURRENT_DATE;

-- Step 2: Insert new active rows for all source rows
INSERT INTO SNOWFLAKE_LEARNING_DB.L2.ITEM_DIM (
    item_key,
    item_name,
    "desc",
    unit_price,
    man_country,
    supplier,
    unit,
    effective_start_date,
    effective_end_date,
    is_current,
    ingest_time
)
SELECT
    item_key,
    item_name,
    "desc",
    unit_price,
    man_country,
    supplier,
    unit,
    CURRENT_DATE,
    NULL,
    TRUE,
    ingest_time
FROM SNOWFLAKE_LEARNING_DB.L1.ITEM_STG;
