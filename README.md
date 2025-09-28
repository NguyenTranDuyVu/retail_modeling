# retail_modeling
Create data pipelines to ingest, transform and model retailing data on Snowflake as well as testing data quality using Airflow and dbt

1. 
create env: uv venv retail-env
install requirements.txt 
airflow, provider snowflake, dbt
create snowflake account
2. 
set up airflow: airflow standalone
AIRFLOW_VERSION=3.0.6
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1,2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

uv pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
export AIRFLOW_HOME="$(pwd)"
airflow standalone

3. install 
uv pip install -r requirements.txt
4. 
ingest data from local system -> staging table snowflake