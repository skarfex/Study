"""
Урок 3
СЛОЖНЫЕ ПАЙПЛАЙНЫ, ЧАСТЬ 1
Задание
"""
from airflow import DAG
import logging
from datetime import datetime
from airflow.utils.dates import days_ago

from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    'owner': 'e-gulymedden-21',
    'poke_interval': 600,
    'start_date': days_ago(2),
    'end_date': datetime(2023, 10, 14)
}

with DAG("e-gulymedden-21-les-3",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['e-gulymedden-21']
         ) as dag:

    dummy = DummyOperator(task_id='start_dummy')

    echo_current_datetime_now = BashOperator(
        task_id='echo_current_datetime_now',
        bash_command='echo {{ macros.datetime.now() }}',
        dag=dag
    )

    def show_execution_date_func(**kwarg):
        logging.info("Show execution date using Python module which called datetime:")
        logging.info(f"{datetime.now()}")
        logging.info("Using JinJa's templates:")
        logging.info(f"{kwarg['ts']}")


    execution_date = PythonOperator(
        task_id='execution_date',
        python_callable=show_execution_date_func,
        dag=dag
    )

    dummy >> [echo_current_datetime_now, execution_date]
