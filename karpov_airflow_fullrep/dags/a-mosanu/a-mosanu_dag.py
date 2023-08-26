"""
Exercise 3.1
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.edgemodifier import Label


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-mosanu',
    'poke_interval': 600
}

with DAG("Chapter_3",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-mosanu']
) as dag:

    dummy = DummyOperator(task_id="DummyOperator")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}'
    )

    def hello_world_func():
        logging.info("My first ever DAG!")

    hello_world = PythonOperator(
        task_id='first',
        python_callable=hello_world_func,
        dag=dag
    )

    dummy >> Label("Echo datetime") >> echo_ds
    dummy >> Label("Python script") >> hello_world