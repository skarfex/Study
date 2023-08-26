from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'r-damdinov-11',
    'poke_interval': 600
}

with DAG("r-damdinov-11_3_1",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['r-damdinov-11']
          ) as dag:

    dummy_op = DummyOperator(
        task_id='dummy',
    )

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo BashOperator log',
    )

    def first_func():
        logging.info("PythonOperator log")


    first_task = PythonOperator(
        task_id='first_task',
        python_callable=first_func,
    )

    dummy_op >> echo_ds >> first_task
