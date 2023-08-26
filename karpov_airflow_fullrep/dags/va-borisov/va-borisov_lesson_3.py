"""
Даг для Урока 3
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
    'owner': ' va-borisov'
}

with DAG("va-borisov_lesson_3",
        schedule_interval='@once',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['lesson_3']
) as dag:

    dummy_task = DummyOperator(task_id='dummy_task')
    bash_ds = BashOperator(task_id='bash_ds',
                           bash_command='echo {{ ds }}')
    
    def print_ds(**kwargs):
        logging.info(kwargs['ds'])

    python_ds = PythonOperator(task_id='python_ds',
                               python_callable=print_ds,
                               op_kwargs={'ds': '{{ ds }}'})
    
    dummy_task >> [bash_ds, python_ds]
