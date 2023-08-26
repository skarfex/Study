"""
Тестовый dag, который
выводит различные переменные окружения

"""
from datetime import timedelta, datetime
import logging

import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, get_current_context
from airflow.operators.bash import BashOperator


DEFAULT_ARGS = {
    'start_date': datetime(2022, 9, 3),
    'owner': 'v-radishevskiy',
    'poke_interval': 600
}

with DAG(
        dag_id="v-radishevskiy-test-dag",
        description="Hellow airflow",
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        schedule_interval='@daily',
        tags=['v-radishevskiy', 'test'],
        catchup=True,
) as dag:
    dummy_start = DummyOperator(task_id='dummy_start')

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def display_airlow_variables():
        context = get_current_context()
        for key, value in context.items():
            logging.info(key)
            logging.info(str(value))

    display_variables = PythonOperator(task_id='display_variables', python_callable=display_airlow_variables)

    lscpu = BashOperator(
        task_id='lscpu',
        bash_command='lscpu',
        dag=dag
    )

    dummy_start >> echo_ds >> display_variables >> lscpu

