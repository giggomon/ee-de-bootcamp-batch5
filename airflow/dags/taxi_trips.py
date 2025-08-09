import subprocess
import logging
import os
from datetime import datetime
from airflow import DAG
# from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator


# Constants
SNOWFLAKE_CONN_ID = "snowflake_conn"
TAXI_TRIP_RAW_TABLE = "TAXI_TRIPS_RAW"
TAXI_TRIP_GCS_STAGE = "GCS_TAXI_STAGE"
TAXI_TRIP_TABLE = "MONICA.TAXI_TRIPS_RAW"
TAXI_TRIP_DB = "EE_SE_DE_DB"

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

# DBT incremental model run
def run_dbt_incremental_model():
    env = os.environ.copy()
    logging.info(f"DBT_SNOWFLAKE_USER={env.get('DBT_SNOWFLAKE_USER')}")
    logging.info(f"DBT_SNOWFLAKE_PWD={'***' if env.get('DBT_SNOWFLAKE_PWD') else None}")
    logging.info(f"DBT_SNOWFLAKE_ACCOUNT={env.get('DBT_SNOWFLAKE_ACCOUNT')}")

    command = [
        'dbt',
        'run',
        '--select', 'taxi_trips_consistent',
        '--project-dir', '/opt/airflow/dbt_project',
        '--profiles-dir', '/opt/airflow/dbt_project',
    ]

    result = subprocess.run(command, capture_output=True, text=True, env=env)
    logging.info(result.stdout)

    if result.returncode != 0:
        logging.warning("DBT incremental run failed")
        logging.warning(result.stdout)
        logging.warning(result.stderr)
    else:
        logging.info("DBT incremental run succeeded")
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

    # 0.1 Check COPY_HISTORY Before Copying
    verify_ingested_files = SQLExecuteQueryOperator(
        task_id="verify_ingested_files",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            SELECT file_name AS filename
            FROM TABLE(
                {TAXI_TRIP_DB}.INFORMATION_SCHEMA.COPY_HISTORY(
                    TABLE_NAME => '{TAXI_TRIP_TABLE}',
                    START_TIME => DATEADD(day, -7, CURRENT_TIMESTAMP())
                )
            );
        """,
        do_xcom_push=True,
    )

    # 1. Copy from GCS stage to Snowflake RAW table
    load_to_snowflake_raw = SQLExecuteQueryOperator(
        task_id="load_to_snowflake_raw",
        sql=f"""
            COPY INTO {TAXI_TRIP_RAW_TABLE} 
            FROM @{TAXI_TRIP_GCS_STAGE}
            FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
            PATTERN = '.*\\.csv'
            ON_ERROR = 'CONTINUE'
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PURGE = FALSE
            FORCE = FALSE;
        """,
        conn_id=SNOWFLAKE_CONN_ID
    )

    # After the COPY task, add this update task:
    update_load_timestamp = SQLExecuteQueryOperator(
        task_id="update_load_timestamp",
        sql=f"""
            UPDATE {TAXI_TRIP_RAW_TABLE}
            SET "created_timestamp" = CURRENT_TIMESTAMP()
            WHERE "created_timestamp" IS NULL;
        """,
        conn_id=SNOWFLAKE_CONN_ID
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

    # 4. Run dbt incremental model
    run_dbt_incremental_task = PythonOperator(
        task_id="run_dbt_incremental_model",
        python_callable=run_dbt_incremental_model,
    )

    # Set dependencies
    log_csv_files_task >> verify_ingested_files >> load_to_snowflake_raw >> update_load_timestamp >> verify_load >> run_dbt_task >> run_dbt_incremental_task
