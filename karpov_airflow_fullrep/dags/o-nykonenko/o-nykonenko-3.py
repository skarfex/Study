from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'o-nykonenko',
    'poke_interval': 600
}

with DAG(
    'first_hard_dag',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['o-nykonenko']
) as dag:

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
    )

    dummyOp = DummyOperator(task_id="dummy")


    def python_op_func():
        logging.info('PythonOperator is working properly')


    pythonOp = PythonOperator(
        task_id='pythonOp',
        python_callable=python_op_func,
    )

    dummyOp >> echo_ds >> pythonOp