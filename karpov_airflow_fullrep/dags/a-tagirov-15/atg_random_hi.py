"""
Тестирую BranchPythonOperator, на выходе нас должен приветствовать рандомный таск каждый день
"""
import random

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-tagirov-15',
    'poke_interval':600
}

with DAG('atg_random_hi',
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['atg']
         ) as dag:
    def select_random_func():
        return random.choice(['first', 'second', 'third'])

    def task_1_func():
        logging.info("Hello, I'm task_1 and I win today")

    def task_2_func():
        logging.info("Hello, I'm task_2 and I win today")

    def task_3_func():
        logging.info("Hello, I'm task_3 and I win today")

    start = DummyOperator(task_id='Start')

    select_random = BranchPythonOperator(
        task_id='select_random',
        python_callable=select_random_func
    )

    task_1 = PythonOperator(
        task_id='first',
        python_callable=task_1_func
    )

    task_2 = PythonOperator(
        task_id='second',
        python_callable=task_2_func
    )

    task_3 = PythonOperator(
        task_id='third',
        python_callable=task_3_func
    )

    finish = DummyOperator(
        task_id='finish',
        trigger_rule='none_failed_or_skipped'
    )

    start >> select_random >> [task_1, task_2, task_3] >> finish