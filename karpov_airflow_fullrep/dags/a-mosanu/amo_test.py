"""

Dag for testing purposes 12-06-2023
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-mosanu',
    'poke_interval': 600
}

with DAG("amo_test",
    schedule_interval='@weekly',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-mosanu']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def hello_world_func():
        logging.info("My first ever DAG!")

    hello_world = PythonOperator(
        task_id='first',
        python_callable=hello_world_func,
        dag=dag
    )

    dummy >> [echo_ds, hello_world]