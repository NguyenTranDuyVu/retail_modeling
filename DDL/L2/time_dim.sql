CREATE TABLE IF NOT EXISTS SNOWFLAKE_LEARNING_DB.L2.TIME_DIM (
    time_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    time_key VARCHAR(20) NOT NULL,
    date TIMESTAMP,
    hour INTEGER,
    day INTEGER,
    week VARCHAR(20),
    month VARCHAR(10),
    quarter VARCHAR(5),
    year INTEGER,

    -- SCD Type 2 metadata
    effective_start_date DATE NOT NULL,
    effective_end_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,

    -- Audit fields
    ingest_time TIMESTAMP
);
