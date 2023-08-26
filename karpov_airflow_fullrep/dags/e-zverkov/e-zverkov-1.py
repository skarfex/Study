"""
Автоматизация ETl-процессов
3 Урок
Домашняя работа
"""



import logging
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
import logging

DEFAULT_ARGS = {
    'owner': 'e-zverkov',
    'start_date': days_ago(2),
    'retries': 3,
    'poke_interval': 600
}

with DAG(
    dag_id='e-zverkov_dag_1',
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',
    max_active_runs=3,
    tags=['e-zverkov'],
    ) as dag:


    first_task = DummyOperator(task_id='first_task')

    def hello_world_func():
        logging.info("Hello, Karpov")

    second_task = PythonOperator(
        task_id='second_task',
        python_callable=hello_world_func,
        provide_context=True
    )

    third_task = BashOperator(
        task_id='end_task',
        bash_command='echo {{ execution_date }}'
    )

    first_task >> second_task >> third_task
