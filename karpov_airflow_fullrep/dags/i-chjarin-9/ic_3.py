"""
Курс Инженер данных
Модуль 4 Автоматизация ETL-процессов
Урок 3 Сложные пайплайны, часть 1
Задание продумать простую логику
"""
import random

from airflow import DAG
from airflow.utils.dates import days_ago
import random
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'i-chjarin-9',
    'poke_interval': 60
}

with DAG("ic_3",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['i-chjarin-9']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    def hello_world_func():
        logging.info("Hello World!")

    def select_random_task():
        return random.choice(['task_1', 'task_2', 'task_3', 'hello_world'])

    random_task = BranchPythonOperator(
        task_id='random_task',
        python_callable=select_random_task,
        dag=dag
    )

    task_1 = DummyOperator(task_id='task_1')
    task_2 = DummyOperator(task_id='task_2')
    task_3 = DummyOperator(task_id='task_3')
    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )

    dummy >> random_task >> [task_1, task_2, task_3, hello_world]