"""
Простой DAG с тремя операторами
Dummy
BashOperator отправляет эхо с датой
PythonOperator отправляет в лог Hello world
"""

from datetime import datetime, timedelta
from multiprocessing import dummy
from tarfile import DEFAULT_FORMAT
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from click import echo

DEFAULT_ARGUMENTS = {
    'start_date': days_ago(2),
    'owner': 'r-atamov',
    'poke_interval': 600,
}

with DAG(
    'r_atamov',
    default_args=DEFAULT_ARGUMENTS,
    description='A simple tutorial DAG',
    schedule_interval='@daily',
    max_active_runs=1,
    tags=['r_atamov'],
) as dag:

    dummy = DummyOperator(task_id="dummy_task")

    echo = BashOperator(
        task_id="echo",
        bash_command='echo {{ ds }}',
    )

    def hello_world_func():
        logging.info('------------------------------->')
        logging.info('Hello wrold!')
        logging.info('-------------------------------<')

    hellow_world = PythonOperator(
        task_id="hellow_world",
        python_callable=hello_world_func,
    )

    dummy >> [echo, hellow_world]
