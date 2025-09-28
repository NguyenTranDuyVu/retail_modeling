-- Step 1: Update existing active rows to inactive
MERGE INTO SNOWFLAKE_LEARNING_DB.L2.TIME_DIM target
USING SNOWFLAKE_LEARNING_DB.L1.TIME_STG source
ON target.time_key = source.time_key
   AND target.is_current = TRUE
WHEN MATCHED THEN
    UPDATE SET
        is_current = FALSE,
        effective_end_date = CURRENT_DATE;

-- Step 2: Insert new active rows for all source rows
INSERT INTO SNOWFLAKE_LEARNING_DB.L2.TIME_DIM (
    time_key,
    date,
    hour,
    day,
    week,
    month,
    quarter,
    year,
    effective_start_date,
    effective_end_date,
    is_current,
    ingest_time
)
SELECT
    time_key,
    date,
    hour,
    day,
    week,
    month,
    quarter,
    year,
    CURRENT_DATE,
    NULL,
    TRUE,
    ingest_time
FROM SNOWFLAKE_LEARNING_DB.L1.TIME_STG;
