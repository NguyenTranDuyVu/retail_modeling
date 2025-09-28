CREATE OR REPLACE TABLE SNOWFLAKE_LEARNING_DB.L1.TIME_STG (
    time_key VARCHAR(20) NOT NULL,
    date TIMESTAMP,
    hour INTEGER,
    day INTEGER,
    week VARCHAR(20),
    month VARCHAR(10),
    quarter VARCHAR(5),
    year INTEGER,
    ingest_time TIMESTAMP
);
