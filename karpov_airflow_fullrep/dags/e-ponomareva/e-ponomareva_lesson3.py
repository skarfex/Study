"""
Тестовый даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from airflow.hooks.base_hook import BaseHook


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'e-ponomareva',
    'poke_interval': 600
}

with DAG("connection_test",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['e-ponomareva_lesson3.1']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def get_loginfunc():
        logging.info(BaseHook.get_connection('conn_karpov_mysql').login)

    get_login = PythonOperator(
        task_id='get_login',
        python_callable=get_loginfunc,
        dag=dag
    )

    def hello_world_func():
        logging.info("Hello World!")

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )

    dummy >> get_login >> [echo_ds, hello_world]