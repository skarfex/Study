"""
Тест
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm-sidkov-11',
    'poke_interval': 600
}

with DAG(dag_id="m-sidkov-11-test",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['m-sidkov-11']
) as dag:

    dummy = DummyOperator(task_id="dummy-sidkov")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )
    def hello_world_func():
        logging.info("hello_world")

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )
dummy >> [echo_ds, hello_world]