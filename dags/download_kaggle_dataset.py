from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import shutil
import kagglehub

# Define default DAG arguments
default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

# Define the function to run in PythonOperator
def download_and_move_dataset():
    # Step 1: Download to kagglehub's default cache
    path = kagglehub.dataset_download("mmohaiminulislam/ecommerce-data-analysis")
    print("Dataset downloaded to:", path)

    # Step 2: Create target directory in current folder
    target_dir = os.path.join(os.getcwd(), "data")
    os.makedirs(target_dir, exist_ok=True)

    # Step 3: Move dataset to ./data/
    destination_path = os.path.join(target_dir, "ecommerce-data")
    if not os.path.exists(destination_path):
        shutil.copytree(path, destination_path)

    print("Dataset moved to:", destination_path)

# Define the DAG
with DAG(
    dag_id="download_kaggle_dataset",
    default_args=default_args,
    description="Download a Kaggle dataset and move it to a data folder",
    schedule=None,  # Trigger manually
    catchup=False,
    tags=["ecommerce"],
) as dag:

    download_task = PythonOperator(
        task_id="download_and_move_dataset",
        python_callable=download_and_move_dataset,
    )

    download_task
