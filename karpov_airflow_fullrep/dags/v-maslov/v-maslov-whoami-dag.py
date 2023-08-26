"""
Тестовый даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v-maslov',
    'poke_interval': 600
}

with DAG("v_maslov_whoami",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['v-maslov']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_whoami = BashOperator(
        task_id='echo_whoami',
        bash_command='whoami',
    )

    def print_whoami_func():
        logging.info("v-maslov")

    print_whoami = PythonOperator(
        task_id='print_whoami',
        python_callable=print_whoami_func,
    )

    dummy >> [echo_whoami, print_whoami]