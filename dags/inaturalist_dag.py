from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from include.inat_api_pull import main
from include.fivetran_sync import run_fivetran_sync


from datetime import datetime, timedelta

default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'inaturalist_pipeline',
    default_args=default_args,
    description='ETL pipeline for iNaturalist data',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    run_initial = PythonOperator(
        task_id="run_initial_run_py",
        python_callable=main,
        op_kwargs={"initial": False},  # pass arguments
    )
      # Example CLI for initial run

     # Wait for S3 synce
    # Fivetran Sync via Python and Airflow Connection
    fivetran_resync = PythonOperator(
        task_id="fivetran_sync",
        python_callable=run_fivetran_sync,
        op_kwargs={"connector_id": "leaf_militia"},
    )


    run_dbt = BashOperator(
        task_id='run_dbt_transform',
        bash_command='cd ${AIRFLOW_HOME}/include/inaturalist_project && dbt run --project-dir .',
    )

    # Set dependencies
    run_initial >> fivetran_resync >> run_dbt 
