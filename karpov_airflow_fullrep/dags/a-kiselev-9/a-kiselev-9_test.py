"""
Тестовый даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import dag, task

DEFAULT_ARGS = {
    'start_date': datetime.date(2022, 3, 1),
    'owner': 'a-kiselev',
    'poke_interval': 600,
    'schedule_interval': '0 0 * * 1-6'
}

@dag("a-kiselev-9_test", default_args = DEFAULT_ARGS,
         max_active_runs = 1, tags = ['a-kiselev'])



    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def hello_world_func():
        logging.info("Hello World!")

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )

    dummy >> [echo_ds, hello_world]