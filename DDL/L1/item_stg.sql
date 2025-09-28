CREATE OR REPLACE TABLE SNOWFLAKE_LEARNING_DB.L1.ITEM_STG (
    item_key VARCHAR(20) NOT NULL,
    item_name VARCHAR(255),
    "desc" VARCHAR(255), -- "desc" is a reserved keyword, so it's quoted
    unit_price FLOAT,
    man_country VARCHAR(100),
    supplier VARCHAR(255),
    unit VARCHAR(50),
    ingest_time TIMESTAMP
);
