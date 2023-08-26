"""
First DAG in Karpov courses education. Create by v-galashov
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'v-galashov',
    'poke_interval': 100
}

with DAG('v-galashov_lesson_3_first_dag',
    schedule_interval='@hourly',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['v-galashov']
) as dag:

    dummyOp = DummyOperator(task_id="dummyOp")

    bashOp = BashOperator(
        task_id='echoPrint',
        bash_command='echo Wow! Its works!'
    )

    def python_op_func():
        logging.info('Execution date is {{ ds }}')

    pythonOp = PythonOperator(
        task_id='executionDatePrint',
        python_callable=python_op_func,
    )

    dummyOp >> bashOp >> pythonOp