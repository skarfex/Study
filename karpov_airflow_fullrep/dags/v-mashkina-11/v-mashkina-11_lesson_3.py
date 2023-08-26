from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
import random

from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v-mashkina-11',
    'poke_interval': 600
}

with DAG("task_1",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['v-mashkina-11']
          ) as dag:

    task_1_start = DummyOperator(task_id='task_1_start')

    task_1_bash = BashOperator(
        task_id='task_1_bash',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def hello_world_func():
        logging.info("Hello world")


    task_1_python = PythonOperator(
        task_id='task_1_python',
        python_callable=hello_world_func,
        dag=dag
    )

    def select_random_func():
        return random.choice(['task_1', 'task_2', 'task_3'])

    task_1 = DummyOperator(task_id='task_1')
    task_2 = DummyOperator(task_id='task_2')
    task_3 = DummyOperator(task_id='task_3')

    task_1_select = BranchPythonOperator(
        task_id='task_1_select',
        python_callable=select_random_func
    )




    task_1_end = DummyOperator(
        task_id='task_1_end',
        trigger_rule='one_success'
    )
    task_1_start >> task_1_bash >> task_1_python >> task_1_select >> [[task_1, task_2, task_3]] >> task_1_end