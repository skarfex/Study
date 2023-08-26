"""
Первый даг для задания 3 урока
5. Внутри создать даг из нескольких тасков, на своё усмотрение:
— DummyOperator
— BashOperator с выводом даты
— PythonOperator с выводом даты
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-belobratova',
    'poke_interval': 600
}

with DAG(
    dag_id="a-belobratova-3",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-belobratova']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}'
    )

    def print_ds_func(**kwargs):
        execution_dt = kwargs['templates_dict']['execution_dt']
        logging.info(execution_dt)

    print_ds = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds_func,
        templates_dict={'execution_dt': '{{ds}}'}
    )

    dummy >> [echo_ds, print_ds]