from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from include.inat_api_pull import main
from include.fivetran_sync import run_fivetran_sync
from datetime import datetime, timedelta

# Default arguments for all tasks in the DAG
default_args = {
    'owner': 'you',  # DAG owner
    'depends_on_past': False,  # Do not wait for previous DAG run
    'retries': 1,  # Retry once on failure
    'retry_delay': timedelta(minutes=5),  # Wait 5 minutes before retry
}

# Define the DAG
with DAG(
    'inaturalist_pipeline',
    default_args=default_args,
    description='ETL pipeline for iNaturalist data',
    schedule='@daily',  # Runs once per day
    start_date=datetime(2025, 1, 1),  # First DAG run date
    catchup=False,  # Do not run past missed schedules
) as dag:

    # Step 1: Pull data from iNaturalist API
    run_initial = PythonOperator(
        task_id="run_initial_run_py",
        python_callable=main,  # Function to pull API data
        op_kwargs={"initial": False},  # Pass argument to indicate incremental vs initial load
    )
    # Example: initial=False runs incremental updates instead of full reload

    # Step 2: Sync S3 data via Fivetran
    fivetran_resync = PythonOperator(
        task_id="fivetran_sync",
        python_callable=run_fivetran_sync,  # Function to trigger Fivetran connector
        op_kwargs={"connector_id": "leaf_militia"},  # Specific Fivetran connector ID
    )
    # Ensures the latest S3 data is synced to the warehouse before transformation

    # Step 3: Run dbt transformations
    run_dbt = BashOperator(
        task_id='run_dbt_transform',
        bash_command='cd ${AIRFLOW_HOME}/include/inaturalist_project && dbt run --project-dir .',
    )
    # Executes dbt models to transform raw data into analytics-ready tables

    # Define task dependencies
    run_initial >> fivetran_resync >> run_dbt

