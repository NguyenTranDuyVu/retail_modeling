CREATE TABLE IF NOT EXISTS SNOWFLAKE_LEARNING_DB.L2.STORE_DIM (
    store_sk NUMBER AUTOINCREMENT PRIMARY KEY, -- Surrogate key
    store_key VARCHAR(20) NOT NULL,            -- Business key
    division VARCHAR(100),
    district VARCHAR(100),
    upazila VARCHAR(100),

    -- SCD Type 2 metadata
    effective_start_date DATE NOT NULL,
    effective_end_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,

    -- Audit fields
    ingest_time TIMESTAMP
);
