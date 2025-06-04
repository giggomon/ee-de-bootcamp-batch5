from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Constants
SNOWFLAKE_CONN_ID = "snowflake_conn"
SNOWFLAKE_DATABASE = "EE_SE_DE_DB"
SNOWFLAKE_SCHEMA = "MONICA"
TAXI_TRIP_RAW_TABLE = "TAXI_TRIPS_RAW"
TAXI_TRIP_GCS_STAGE = "GCS_TAXI_STAGE"

with DAG(
    dag_id = "load_data_taxi_trips",
    description = "DAG for loading taxi trips data from GCS bucket to Snowflake",
    start_date=datetime(2021, 1, 1),
    tags=["snowflake", "gcs"],
    schedule_interval=None,
    catchup=False,
) as dag:

    # Copy from GCS stage to Snowflake table
    load_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_to_snowflake",
        table=f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TAXI_TRIP_RAW_TABLE}",
        stage=f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TAXI_TRIP_GCS_STAGE}",
        file_format=f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.CSV_FORMAT",
        pattern=".*\\.csv",
        copy_options="ON_ERROR = 'CONTINUE', MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE, FORCE = FALSE",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    # Row Counts
    verify_load = SQLExecuteQueryOperator(
        task_id="verify_load",
        sql=f"SELECT COUNT(*) AS row_count FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TAXI_TRIP_RAW_TABLE}",
        do_xcom_push=True,
        conn_id=SNOWFLAKE_CONN_ID,
    )

    # Set dependencies
    load_to_snowflake >> verify_load