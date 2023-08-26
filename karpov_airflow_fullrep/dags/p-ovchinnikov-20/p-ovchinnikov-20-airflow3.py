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
    'owner': 'p-ovchinnikov-20',
    'poke_interval': 600
}

with DAG("p-ovchinnikov-20",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['p-ovchinnikov-20']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def get_dt():
        logging.info("Hello World!")

    get_dt = PythonOperator(
        task_id='get_dt',
        python_callable=get_dt,
        dag=dag
    )

    dummy >> [echo_ds, get_dt]

