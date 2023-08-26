from airflow import DAG
from datetime import timedelta, datetime
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 11, 5, 10, 00),
    'owner': 'p-pavljukov-14',
    'poke_interval': 400
}

with DAG(
        dag_id='p-pavljukov-14',
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['p-pavljukov-14']
        ) as dag:

    start_task = DummyOperator(task_id='start_task')

    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo {{ ds }}',
        dag=dag
    )


    def python_func():
        logging.info("python_task")


    python_task = PythonOperator(
        task_id='python_task',
        python_callable=python_func,
        dag=dag
    )

    start_task >> bash_task >> python_task