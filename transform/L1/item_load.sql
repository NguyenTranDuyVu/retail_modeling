        COPY INTO SNOWFLAKE_LEARNING_DB.L1.ITEM_STG (
            ITEM_KEY,
            ITEM_NAME,
            "desc",
            UNIT_PRICE,
            MAN_COUNTRY,
            SUPPLIER,
            UNIT,
            INGEST_TIME
        )
        FROM (
            SELECT
                $1,  
                $2,  
                $3,  
                $4,  
                $5,  
                $6,  
                $7,
                CURRENT_TIMESTAMP()
            FROM '@{{params.STAGE_NAME}}/{{params.STAGE_FILE_NAME}}'
        )
        FILE_FORMAT = (
            TYPE = 'CSV',
            FIELD_OPTIONALLY_ENCLOSED_BY = '"',
            SKIP_HEADER = 1
        )
        ON_ERROR = 'CONTINUE'
        FORCE = TRUE;