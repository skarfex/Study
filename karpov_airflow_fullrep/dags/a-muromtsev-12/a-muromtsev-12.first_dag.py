from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

import logging
import random

DEFAULT_ARGS = {
    'owner': 'a-muromtsev-12',
    'retries': 3,
    'start_date': days_ago(2)
}


def random_choice_func():
    return random.choice(['task_1', 'task_2', 'task_3'])


def task_number_func(*op_args):
    logging.info(f"this is task_{op_args[0]}")


with DAG(
        dag_id="a-muromtsev-12.first_dag",
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['karpov']
) as dag:
    start = DummyOperator(task_id='start')

    datetime = BashOperator(
        task_id="datetime",
        bash_command='echo current date and time: {{ds_nodash}}'
    )

    select_random = BranchPythonOperator(
        task_id='select_random',
        python_callable=random_choice_func
    )

    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=task_number_func,
        op_args=[1]
    )

    task_2 = PythonOperator(
        task_id='task_2',
        python_callable=task_number_func,
        op_args=[2]
    )

    task_3 = PythonOperator(
        task_id='task_3',
        python_callable=task_number_func,
        op_args=[3]
    )

start >> datetime >> select_random >> [task_1, task_2, task_3]
