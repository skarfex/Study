"""
Даг из нескольких тасков:
— DummyOperator
— BashOperator с выводом строки
— PythonOperator с выводом строки
— любая другая простая логика
"""

import random
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's-jushanov-9',
    'poke_interval': 600
}

with DAG(
    "s_jushanov_02_03_task",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['s-jushanov-9']
) as dag:

    start_task = DummyOperator(
        task_id='start_task'
    )

    echo_exdate_task = BashOperator(
        task_id='echo_exdate_task',
        bash_command='echo {{ execution_date }}',
    )

    echo_ds_task = BashOperator(
        task_id='echo_ds_task',
        bash_command='echo {{ ds }}',
    )

    def logging_func():
        logging.info("Python Operator")

    logging_task = PythonOperator(
        task_id='logging_task',
        python_callable=logging_func,
    )

    def select_random_func():
        return random.choice(['echo_exdate_task', 'echo_ds_task', 'logging_task'])

    select_random_task = BranchPythonOperator(
        task_id='select_random_task',
        python_callable=select_random_func
    )

    start_task >> select_random_task >> [echo_exdate_task, echo_ds_task, logging_task]
