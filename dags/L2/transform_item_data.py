from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import snowflake.connector
from pathlib import Path
import uuid

# Snowflake connection info
SNOWFLAKE_CONN_ID = "my_snowflake_conn"

cwd = Path.cwd()
DDL_SEARCHPATH = cwd / 'DDL' / 'L2' 
TRANSFORM_SEARCHPATH = cwd / 'transform' / 'L2'
STAGE_NAME = "~"

with DAG(
    dag_id="transform_item_data",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce"],
    template_searchpath=[DDL_SEARCHPATH, TRANSFORM_SEARCHPATH]
) as dag:
    
    create_table_task = SQLExecuteQueryOperator(
        task_id="create_table",
        sql='item_dim.sql',
        conn_id=SNOWFLAKE_CONN_ID
    )

    merge_task = SQLExecuteQueryOperator(
        task_id="merge_item_data", sql='item_transform.sql', conn_id=SNOWFLAKE_CONN_ID,
    )

    create_table_task >> merge_task
