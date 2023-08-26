"""
DAG для урока 3
"""
import random
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's-mokrushin',
    'poke_interval': 600
}

with DAG("s-mokrushin_3_dag",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['s-mokrushin']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',

    )

    def hello_world_func():
        logging.info("Hello World!")

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,

    )


    def select_random_function():
        return random.choice(['echo_ds', 'hello_world'])


    select_random = BranchPythonOperator(
        task_id='select_random',
        python_callable=select_random_function
    )

    dummy >> select_random >> [echo_ds, hello_world]