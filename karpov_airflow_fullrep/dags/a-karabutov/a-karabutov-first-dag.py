"""
testing
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-karabutov',
    'poke_interval': 600
}

with DAG('a-karabutov-first-dag',
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-karabutov']
         ) as dag:
    dummy = DummyOperator(
        task_id='dummy'
    )

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',  # выполняемый bash script (ds = execution_date)
        dag=dag
    )


    def hello_word_func():
        logging.info('Hello World')


    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_word_func,  # ссылка на функцию, выполняемую в рамках таски
        dag=dag
    )

    dummy >> [echo_ds, hello_world]
