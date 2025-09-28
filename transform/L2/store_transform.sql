-- Step 1: Expire existing active records if changed
MERGE INTO SNOWFLAKE_LEARNING_DB.L2.STORE_DIM target
USING SNOWFLAKE_LEARNING_DB.L1.STORE_STG source
ON target.store_key = source.store_key
   AND target.is_current = TRUE
WHEN MATCHED THEN
    UPDATE SET
        is_current = FALSE,
        effective_end_date = CURRENT_DATE;

-- Step 2: Insert new records from staging as current
INSERT INTO SNOWFLAKE_LEARNING_DB.L2.STORE_DIM (
    store_key,
    division,
    district,
    upazila,
    effective_start_date,
    effective_end_date,
    is_current,
    ingest_time
)
SELECT
    store_key,
    division,
    district,
    upazila,
    CURRENT_DATE,
    NULL,
    TRUE,
    ingest_time
FROM SNOWFLAKE_LEARNING_DB.L1.STORE_STG
WHERE store_key NOT IN (
    SELECT store_key FROM SNOWFLAKE_LEARNING_DB.L2.STORE_DIM WHERE is_current = TRUE
)
OR EXISTS (
    SELECT 1
    FROM SNOWFLAKE_LEARNING_DB.L2.STORE_DIM dim
    WHERE dim.store_key = SNOWFLAKE_LEARNING_DB.L1.STORE_STG.store_key
      AND dim.is_current = FALSE
);
