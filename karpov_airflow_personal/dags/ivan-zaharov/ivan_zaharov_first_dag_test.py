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
    'owner': 'ivan-zaharov',
    'poke_interval': 600
}

with DAG("ivan_zaharov_first_dag_test",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['ivan_zaharov']
         ) as dag:

    dummy = DummyOperator(
        task_id='dummy'
    )

    echo_ds = BashOperator(
        task_id='echo_ds',
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

    dummy >> [echo_ds, hello_world]
