"""
Тестовый даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm-kochenjuk-10',
    'poke_interval': 600
}

with DAG("m-kochenjuk-10_test",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['m-kochenjuk-10']) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id='end')

    echo_date = BashOperator(
        task_id='print_date',
        bash_command='date',
        dag=dag
    )
    
    get_executor = BashOperator(
        task_id='get_executor',
        bash_command='airflow config get-value core executor',
        dag=dag
    )

    def hello_world_func():
        logging.info("Hello World!")

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )

    start >> [echo_date, hello_world] >> get_executor >> end