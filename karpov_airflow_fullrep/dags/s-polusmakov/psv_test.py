"""
Тестовый даг
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's-polusmakov',
    'poke_interval': 600
}

with DAG("psv_test",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['s-polusmakov']
         ) as dag:

    dummy = DummyOperator(task_id="dummy")


    echo_psv = BashOperator(
        task_id='echo_psv',
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



    dummy >> [echo_psv, hello_world]
