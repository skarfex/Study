"""
Даг для урока 3.
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'm-stratonnikov',
    'depends_on_past': False,
    'start_date': days_ago(5),
    'email': ['maxstrato@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG("m-stratonnikov_lesson_3",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['m-stratonnikov_tag']
) as dag:

    start_task = DummyOperator(task_id='start_task')

    first_str = BashOperator(
        task_id = 'first_str',
        bash_command = "echo 'Wow, my first dag says:'"
        )

    def second_str_func():
        logging.info("Hello World!")

    second_str = PythonOperator(
        task_id='second_str',
        python_callable=second_str_func
        )

    def third_str_func():
        logging.info("!!")

    third_str = PythonOperator(
        task_id='third_str',
        python_callable=third_str_func
        )


    start_task >> first_str >> second_str >> third_str