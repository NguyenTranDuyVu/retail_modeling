from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.base import BaseHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import snowflake.connector
from pathlib import Path

# Snowflake connection ID
SNOWFLAKE_CONN_ID = "my_snowflake_conn"

# Local paths
cwd = Path.cwd()
DDL_SEARCHPATH = cwd / 'DDL' / 'L1'
TRANSFORM_SEARCHPATH = cwd / 'transform' / 'L1'
LOCAL_FILE_PATH = cwd / "data" / "ecommerce-data" /"Trans_dim.csv"

# Snowflake stage
STAGE_FILE_NAME = "Trans_dim.csv"
STAGE_NAME = "~"

# Upload file to Snowflake stage
def upload_to_snowflake_stage():
    snowflake_conn = BaseHook.get_connection(SNOWFLAKE_CONN_ID)

    extra = snowflake_conn.extra_dejson
    conn = snowflake.connector.connect(
        user=snowflake_conn.login,
        password=snowflake_conn.password,
        account=extra.get('account'),
        warehouse=extra.get('warehouse'),
        database=extra.get('database')
    )
    cursor = conn.cursor()
    try:
        put_query = f"PUT file://{LOCAL_FILE_PATH} @{STAGE_NAME} OVERWRITE = TRUE;"
        cursor.execute(put_query)
    finally:
        cursor.close()
        conn.close()

# DAG definition
with DAG(
    dag_id="load_transaction_data",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce"],
    template_searchpath=[DDL_SEARCHPATH, TRANSFORM_SEARCHPATH]  # ✅ Added here
) as dag:
    
    create_table_task = SQLExecuteQueryOperator(
        task_id="create_table",
        sql='transaction_stg.sql',
        conn_id=SNOWFLAKE_CONN_ID
    )

    upload_task = PythonOperator(
        task_id="upload_to_snowflake_stage",
        python_callable=upload_to_snowflake_stage,
    )
    
    copy_task = SQLExecuteQueryOperator(
        task_id="copy_into_table",
        sql='transaction_load.sql',
        conn_id=SNOWFLAKE_CONN_ID,
        params={"STAGE_NAME": STAGE_NAME, "STAGE_FILE_NAME": STAGE_FILE_NAME}
    )

    trigger_transform_task = TriggerDagRunOperator(
        task_id="trigger_transform_transaction",
        trigger_dag_id="transform_transaction_data",
        wait_for_completion=False,
    )

    create_table_task >> upload_task >> copy_task >> trigger_transform_task
