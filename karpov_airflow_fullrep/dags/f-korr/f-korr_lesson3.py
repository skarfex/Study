"""
Lesson 3 part 1
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'f-korr',
    'poke_interval': 300
}

with DAG('f-korr_lesson3',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['f-korr', 'ht5o5KnE']
) as dag:

    dummyOp = DummyOperator(task_id="dummy")

    bashOp = BashOperator(
        task_id='bash_id',
        bash_command='echo BashOperator {{ ds }}! I hope its work!',
        dag=dag
    )

    def python_op_func():
        logging.info('echo PythonOperator! I hope its work!')

    pythonOp = PythonOperator(
        task_id='Python_Op_Id',
        python_callable=python_op_func,
        dag=dag
    )

    dummyOp >> bashOp >> pythonOp