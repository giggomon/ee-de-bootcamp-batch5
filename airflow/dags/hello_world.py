# This DAG include two tasks: (1) write_to_gcs: writes the hello_world.txt file to GCS bucket (2) read_from_gcs: read the file from GCS and prints its content

from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator

# Replace this with your GCS bucket name
GCS_BUCKET_NAME = "se-data-landing-monica"


# Function to write to GCS
def write_to_gcs():
    hook = GCSHook()
    file_name = "hello_world.txt"
    file_content = "Hello, World from Airflow!"

    # Upload the file to the bucket
    hook.upload(
        bucket_name=GCS_BUCKET_NAME,
        object_name=file_name,
        data=file_content
    )
    print(f"{file_name} written to bucket {GCS_BUCKET_NAME}")


# Function to read from GCS
def read_from_gcs():
    hook = GCSHook()
    file_name = "hello_world.txt"

    # Download the file content
    content = hook.download(
        bucket_name=GCS_BUCKET_NAME,
        object_name=file_name
    )
    print(f"Content of {file_name}: {content.decode('utf-8')}")


# Define the DAG
with DAG(
        dag_id="hello_world_gcs",
        start_date=datetime(2023, 1, 1),
        schedule_interval=None,
        catchup=False,
        tags=["example"],
) as dag:
    # Task 1: Write to GCS
    write_task = PythonOperator(
        task_id="write_to_gcs",
        python_callable=write_to_gcs
    )

    # Task 2: Read from GCS
    read_task = PythonOperator(
        task_id="read_from_gcs",
        python_callable=read_from_gcs
    )

    # Task Dependencies
    write_task >> read_task