from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def hello_world():
    print("Hello World!")

dag = DAG(
    'hello_world',
    default_args=default_args,
    description='A simple Hello World DAG',
    schedule_interval=timedelta(days=1),
)

hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=hello_world,
    dag=dag,
)