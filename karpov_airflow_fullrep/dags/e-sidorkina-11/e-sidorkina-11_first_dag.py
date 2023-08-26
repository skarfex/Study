"""
Тестовый простой даг - задание к уроку 3
"""

import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

def task_success_alert(context):
    print(f"DAG has succeeded, run_id: {context['run_id']}")

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'end_date': datetime(2022, 9, 30),
    'owner': 'e-sidorkina-11',
    'poke_interval': 600,
    'on_success_callback': task_success_alert
}

with DAG("e-sidorkina-11_first_dags",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['e-sidorkina-11']
         ) as dag:
    dummy = DummyOperator(task_id="dummy")

    log_ds = BashOperator(
        task_id='log_ds',
        bash_command='echo {{ ts }}',
        dag=dag
    )

    def hello_world_func():
        logging.info("Hello World!")


    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )

    dummy >> [hello_world, log_ds]
