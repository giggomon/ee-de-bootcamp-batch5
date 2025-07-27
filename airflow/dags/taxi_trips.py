import subprocess
import logging
import os
from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator


# Constants
SNOWFLAKE_CONN_ID = "snowflake_conn"
TAXI_TRIP_RAW_TABLE = "TAXI_TRIPS_RAW"
TAXI_TRIP_GCS_STAGE = "GCS_TAXI_STAGE"

# DBT model run
def run_dbt_model():
    env = os.environ.copy()
    logging.info(f"DBT_SNOWFLAKE_USER={env.get('DBT_SNOWFLAKE_USER')}")
    logging.info(f"DBT_SNOWFLAKE_PWD={'***' if env.get('DBT_SNOWFLAKE_PWD') else None}")
    logging.info(f"DBT_SNOWFLAKE_ACCOUNT={env.get('DBT_SNOWFLAKE_ACCOUNT')}")

    command = [
        'dbt',
        'run',
        '--select', 'stg_taxi_trips_consistent',
        '--project-dir', '/opt/airflow/dbt_project',
        '--profiles-dir', '/opt/airflow/dbt_project',
    ]

    result = subprocess.run(command, capture_output=True, text=True, env=env)
    logging.info(result.stdout)

    if result.returncode != 0:
        logging.warning("DBT run failed")
        logging.warning(result.stdout)
        logging.warning(result.stderr)
    else:
        logging.info("DBT run succeeded")
        logging.info(result.stdout)

# Pre-check task that queries the stage and logs which files are being picked up
def log_csv_files_in_stage():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    query = "LIST @{};".format(TAXI_TRIP_GCS_STAGE)
    results = hook.get_pandas_df(query)

    matched_files = results[results['name'].str.lower().str.endswith('.csv')]['name'].tolist()

    if matched_files:
        logging.info("CSV files found in stage:")
        for f in matched_files:
            logging.info(f" - {f}")
    else:
        logging.info("No CSV files found in stage.")

# Define the DAG
with DAG(
    dag_id = "load_data_taxi_trips",
    description = "DAG for loading taxi trips data from GCS bucket to Snowflake RAW table",
    start_date=datetime(2021, 1, 1),
    tags=["snowflake", "gcs"],
    schedule_interval=None,
    catchup=False,
) as dag:

    # 0. Pre-load file logging
    log_csv_files_task = PythonOperator(
        task_id="log_csv_files_in_stage",
        python_callable=log_csv_files_in_stage,
    )
    
    # 1. Copy from GCS stage to Snowflake RAW table
    load_to_snowflake_raw = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_to_snowflake_raw",
        table=TAXI_TRIP_RAW_TABLE,
        stage=TAXI_TRIP_GCS_STAGE,
        file_format="CSV_FORMAT",
        pattern=".*\\.csv",
        copy_options="ON_ERROR = 'CONTINUE', MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE, FORCE = FALSE",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    # 2. Verify load by checking row count
    verify_load = SQLExecuteQueryOperator(
        task_id="verify_load",
        sql=f"SELECT COUNT(*) AS row_count FROM {TAXI_TRIP_RAW_TABLE}",
        do_xcom_push=True,
        return_last=True,
        conn_id=SNOWFLAKE_CONN_ID,
    )

    # 3. Run dbt model
    run_dbt_task = PythonOperator(
        task_id="run_dbt_model",
        python_callable=run_dbt_model,
    )

    # Set dependencies
    log_csv_files_task >> load_to_snowflake_raw >> verify_load >> run_dbt_task
