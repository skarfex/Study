""" Lessons 3-4 test DAG"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import datetime as dt

from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'owner': 'al-a-21',
    'queue': 'al-a-21',
    'start_date': days_ago(2),
    'retries': 3,
    'execution_timeout': dt.timedelta(seconds=300)
}

with DAG(
    dag_id='al-a-21',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['al-a-21']
) as dag:

    dummy = DummyOperator(task_id='dummy-task')

    dummy2 = DummyOperator(task_id='dummy-task-2')

    task1 = BashOperator(
        task_id='output-date-bash-task',
        bash_command='echo {{ ds }}'
    )
    def log_date(**kwargs):
        execution_date = kwargs['ds']
        logging.info(f'Execution date is {execution_date}')
    task2 = PythonOperator(
        task_id='output-date-python-task',
        python_callable=log_date
    )

    dummy >> dummy2 >> task1 >> task2
