"""
    Задание 3
"""
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    'owner': 'p-abramov-17',
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(seconds=5),
    'start_date': days_ago(2),
    'poke_interval': 300,
    'trigger_rule': 'all_success'
}

with DAG('p_abramov_17_lesson3',
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['p_abramov_17_lesson3']
         ) as dag:
    dummy = DummyOperator(task_id="dummy")

    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='now=$(date "+%Y %m %d %H %M %S"); echo $now'
    )

    def python_func():
        t_ = datetime.today()
        print(f'Если верить Python, то сейчас: + {t_}')


    python_task = PythonOperator(
        task_id='python_task',
        python_callable=python_func
    )


    dummy >> python_task >> bash_task
