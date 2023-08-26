"""
Даг из задания к 3-му уроку
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-pshenov',
    'poke_interval': 600
}

with DAG("a-pshenov-lesson-3",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-pshenov']
) as dag:


    first_task = DummyOperator(task_id="first_task")

    bash_task = BashOperator(task_id='bash_task', bash_command='echo "this task started in {{ ds }}"')

    def args_func(**kwargs):
        logging.info('context, {{ts}}:' + kwargs['ts'])

    print_args_func = PythonOperator(
        task_id='print_args',
        python_callable=args_func,
        provide_context=True
    )

    first_task >> bash_task >> print_args_func


