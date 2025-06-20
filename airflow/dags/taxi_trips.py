from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Constants
SNOWFLAKE_CONN_ID = "snowflake_conn"
TAXI_TRIP_STAGING_TABLE = "TAXI_TRIPS_STAGING"
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

    # 1. Copy from GCS stage to Snowflake table
    load_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_to_snowflake",
        table=TAXI_TRIP_STAGING_TABLE,
        stage=TAXI_TRIP_GCS_STAGE,
        file_format="CSV_FORMAT",
        pattern=".*\\.csv",
        copy_options="ON_ERROR = 'CONTINUE', MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE, FORCE = FALSE",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    # 2. Insert from staging to final table, letting Snowflake set created_timestamp
    insert_into_final = SQLExecuteQueryOperator(
        task_id="insert_into_final",
        sql=f"""
            INSERT INTO {TAXI_TRIP_RAW_TABLE} (
                "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime" , "passenger_count" , "trip_distance" ,"pickup_longitude", "pickup_latitude", "RatecodeID", "store_and_fwd_flag", "dropoff_longitude", "dropoff_latitude", "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount"
            )
            SELECT
                "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime" , "passenger_count" , "trip_distance" ,"pickup_longitude", "pickup_latitude", "RatecodeID", "store_and_fwd_flag", "dropoff_longitude", "dropoff_latitude", "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount"
            FROM {TAXI_TRIP_STAGING_TABLE};
            """,
        conn_id=SNOWFLAKE_CONN_ID,
    )

    # 3. Row Counts
    verify_load = SQLExecuteQueryOperator(
        task_id="verify_load",
        sql=f"SELECT COUNT(*) AS row_count FROM {TAXI_TRIP_RAW_TABLE}",
        do_xcom_push=True,
        conn_id=SNOWFLAKE_CONN_ID,
    )

    # Set dependencies
    load_to_snowflake >> insert_into_final >> verify_load