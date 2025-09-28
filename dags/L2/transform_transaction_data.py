from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
from pathlib import Path

# Snowflake connection ID
SNOWFLAKE_CONN_ID = "my_snowflake_conn"

# Local paths
cwd = Path.cwd()
DDL_SEARCHPATH = cwd / 'DDL' / 'L2'
TRANSFORM_SEARCHPATH = cwd / 'transform' / 'L2'

# DAG definition
with DAG(
    dag_id="transform_transaction_data",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce"],
    template_searchpath=[DDL_SEARCHPATH, TRANSFORM_SEARCHPATH]  # ✅ Added here
) as dag:
    
    create_table_task = SQLExecuteQueryOperator(
        task_id="create_table",
        sql='transaction_dim.sql',
        conn_id=SNOWFLAKE_CONN_ID
    )

    merge_task = SQLExecuteQueryOperator(
        task_id="merge_transaction_data",
        sql='transaction_transform.sql',
        conn_id=SNOWFLAKE_CONN_ID,
    )

    create_table_task >> merge_task
