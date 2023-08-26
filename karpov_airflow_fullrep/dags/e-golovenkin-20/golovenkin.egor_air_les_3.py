"""
Simple dag (Airflow, les 3)
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'golovenkin.egor'
}

with DAG('e-golovenkin-20-les3',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['gol']
) as dag:

    dummy = DummyOperator(task_id='dummy')

    echo_cur_date = BashOperator(
        task_id='echo_cur_date',
        bash_command='echo {{ macros.datetime.now() }}',
    )

    def execution_date (**kwargs):
        logging.info('execution_date - '+kwargs['ds'])

    python_exec_dt = PythonOperator(
        task_id='python_exec_dt',
        python_callable=execution_date
    )

    dummy >> [echo_cur_date, python_exec_dt]
