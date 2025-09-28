CREATE TABLE IF NOT EXISTS SNOWFLAKE_LEARNING_DB.L2.TRANSACTION_DIM (
    transaction_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    payment_key VARCHAR(20) NOT NULL,
    trans_type VARCHAR(50),
    bank_name VARCHAR(255),

    effective_start_date DATE NOT NULL,
    effective_end_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,

    ingest_time TIMESTAMP
);
