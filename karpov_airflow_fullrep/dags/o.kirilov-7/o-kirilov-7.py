"""
Урок 3. Часть 1
Внутри создать даг из нескольких тасков:
— DummyOperator
— BashOperator с выводом даты
— PythonOperator с выводом даты
"""
#import libraries
from airflow.utils.dates import days_ago
from airflow.decorators import dag
import logging

#import operator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

#defauld_args
DEFAULT_ARGS = {
    'start_date' : days_ago(2),
    'owner' : 'okr',
    'poke_interval' : 600
}

#hard work
@dag(
    dag_id = 'okr_simple_dag_v3',
    schedule_interval = '@daily',
    default_args = DEFAULT_ARGS,
    max_active_runs = 1,
    tags = ['kirilov']
)
def generate_dag():
    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}'
        )
    def hello_world_func():
        logging.info("Hello World!")

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func
    )
    dummy >> [echo_ds, hello_world]
dag = generate_dag()
