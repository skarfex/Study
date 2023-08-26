"""
Это простейший даг.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's-jushanov-9',
    'poke_interval': 600
}

with DAG(
    "s_jushanov_02_02_practice",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['s-jushanov-9']
) as dag:

    dummy = DummyOperator(
        task_id='dummy'
    )

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def hello_world_func():
        logging.info("Hello, World!!!")

    hello_world_task = PythonOperator(
        task_id='hello_world_task',
        python_callable=hello_world_func,
        dag=dag
    )

    dummy >> [echo_ds, hello_world_task]
