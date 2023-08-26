from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import os
import platform
    
import psutil

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'u-latynski',
    'poke_interval': 600
}

def print_env_vars(**kwargs):
    for key, value in os.environ.items():
        print(f"{key}: {value}")
        

def print_os_and_memory(**kwargs):
    print("Operating System:", platform.system())
    print("Free Memory:", psutil.virtual_memory().available / (1024**2), "MB")

with DAG("u_latynski_test_1",
    schedule_interval="@daily",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['u-latynski', 'test1']
) as dag:
    
    dummy = DummyOperator(task_id="dummy")
    
    print_env_task = PythonOperator(
        task_id='print_env_vars',
        python_callable=print_env_vars,
        dag=dag,
    )
    
    print_platform_task = PythonOperator(
        task_id='print_os_and_memory',
        python_callable=print_os_and_memory,
        dag=dag,
    )
    
    dummy >> [print_env_task, print_platform_task]