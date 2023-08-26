"""
Курс "Инженер данных". Модуль 2. Задание к уроку #3.
"""

from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 12, 12, tz='Europe/Moscow'),
    'owner': 'b-matjushin',
    'poke_interval': 600,
    'trigger_rule': 'dummy'
}

with DAG(
    'b-matjushin_hw_2-3',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,  # default_args
    max_active_runs=1,
    tags=['b-matjushin']
) as dag:

    task_dummy = DummyOperator(task_id='task_dummy')

    task_bash = BashOperator(
        task_id='task_bash',
        bash_command='echo {{ ds }}'
    )

    def countdown_func():
        print(f'3... 2.. 1...')

    task_python = PythonOperator(
        task_id='task_python',
        python_callable=countdown_func
    )

    task_dummy >> task_python >> task_bash


