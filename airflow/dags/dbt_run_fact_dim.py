import subprocess
import logging
from unittest import result
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def run_dbt_model():
    command = [
        'dbt',
        'run',
        '--select', 'tag:fact_dim',
        '--project-dir', '/opt/airflow/dbt_project',
        '--profiles-dir', '/opt/airflow/dbt_project',
    ]

    logging.info("Starting DBT model execution...")
    logging.info(f"Running command: {' '.join(command)}")

    result = subprocess.run(command, capture_output=True, text=True)
    
    if result.returncode != 0:
        logging.error(f"DBT run failed:\n{result.stderr}")
        raise Exception(f"DBT run failed:\n{result.stderr}")
    else:
        logging.info("DBT run completed successfully.")
        logging.info(f"Output:\n{result.stdout}")
        
# Define the DAG
with DAG(
    dag_id='dbt_run_fact_and_dimensions',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # or set to None for manual runs
    catchup=False,
    tags=['dbt'],
) as dag:

    run_dbt = PythonOperator(
        task_id='run_dbt_fact_and_dims',
        python_callable=run_dbt_model,
    )