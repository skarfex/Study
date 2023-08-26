"""
Тестовый даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

import logging

DEFAULT_ARGS = {
    "start_date": days_ago(7),
    "owner": "a-bagina-19",
    "poke_interval": 600,
}

with DAG(
    "test_dag",
    schedule_interval="@daily",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["a-bagina-19"],
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(task_id="echo_ds", bash_command="echo {{ ds }}", dag=dag)

    def hello_world_func():
        logging.info("Hello World!")

    hello_world = PythonOperator(
        task_id="hello_world",
        python_callable=hello_world_func,
        dag=dag,
    )

    dummy >> [echo_ds, hello_world]
