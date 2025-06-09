# This DAG include two tasks: (1) write_to_gcs: writes the hello_world.txt file to GCS bucket (2) read_from_gcs: read the file from GCS and prints its content
import logging
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator

# Replace this with your GCS bucket name
GCS_BUCKET_NAME = "se-data-landing-monica"
PROJECT_ID = 'ee-india-se-data'
GCP_CONN_ID = 'google_cloud_default'

"""
Writes a hello world message to Google Cloud Storage bucket.
"""

def write_to_gcs() -> None:
    hook = GCSHook(gcp_conn_id=GCP_CONN_ID, project_id=PROJECT_ID)
    file_name = "hello_world.txt"
    file_content = "Hello, World from Airflow!"

    # Upload the file to the bucket
    hook.upload(
        bucket_name=GCS_BUCKET_NAME,
        object_name=file_name,
        data=file_content
    )
    logging.info(f"{file_name} written to bucket {GCS_BUCKET_NAME}")

# Function to read from GCS
def read_from_gcs() -> None:
    hook = GCSHook(gcp_conn_id=GCP_CONN_ID, project_id=PROJECT_ID)
    file_name = "hello_world.txt"

    # Download the file content
    content = hook.download(
        bucket_name=GCS_BUCKET_NAME,
        object_name=file_name
    )
    logging.info(f"Content of {file_name}: {content.decode('utf-8')}")


# Define the DAG
with DAG(
    dag_id="gcs_connection",
    description = "DAG to demonstrate GCS read/write operations",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "gcs"],
) as dag:

    # test_task = PythonOperator(
    #     task_id='test_gcs_connection',
    #     python_callable=test_gcs_connection,
    # )
    # Task 1: Write to GCS
    write_task = PythonOperator(
        task_id="write_to_gcs",
        python_callable=write_to_gcs,
        retries = 2,  # Add retries for reliability
)

    # Task 2: Read from GCS
    read_task = PythonOperator(
        task_id="read_from_gcs",
        python_callable=read_from_gcs,
        retries=2,
    )

    # Task Dependencies
    write_task >> read_task