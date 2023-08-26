"""
Тестовый даг
Курс Инженер данных
Модуль 4 Автоматизация ETL-процессов
Урок 2 Знакомство с Airflow
Пракутика Создаём простой даг
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'i-chjarin-9',
    'poke_interval': 60
}

with DAG("ic_2_practice",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['i-chjarin-9']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ic = BashOperator(
        task_id='echo_ic',
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

    dummy >> [echo_ic, hello_world]