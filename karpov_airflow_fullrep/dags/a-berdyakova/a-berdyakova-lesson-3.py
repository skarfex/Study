"""
Lesson3:
DAG for printing date

"""

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import logging

DEFAULT_ARGS = {
    'start_date' : days_ago(2),
    'owner' : 'a-berdyakova',
    'poke_interval' : 600
}

with DAG(
    "a-berdyakova-lesson-3",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-berdyakova']
) as dag:

    dummy = DummyOperator(task_id='dummy')

    echo_ds = BashOperator(
        task_id='echo_date',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def print_date_func(ds):
        logging.info(ds)

    print_date = PythonOperator(
        task_id = 'print_date',
        python_callable=print_date_func,
        dag=dag
    )

    dummy >> [echo_ds, print_date]
