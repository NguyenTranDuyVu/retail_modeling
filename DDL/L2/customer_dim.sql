CREATE TABLE IF NOT EXISTS SNOWFLAKE_LEARNING_DB.L2.CUSTOMER_DIM (
    customer_sk NUMBER AUTOINCREMENT PRIMARY KEY, -- Surrogate key
    customer_key VARCHAR(20) NOT NULL,            -- Business key
    name VARCHAR(255),
    contact_no VARCHAR(20),
    nid VARCHAR(20),

    -- SCD Type 2 metadata
    effective_start_date DATE NOT NULL,
    effective_end_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,

    -- Audit fields
    ingest_time TIMESTAMP
);