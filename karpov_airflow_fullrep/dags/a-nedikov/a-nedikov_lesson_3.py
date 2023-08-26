from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
import logging

DEFAULT_ARGS = {
    'owner': 'a-nedikov',
    'start_date': days_ago(2),
    'retries': 3,
    'poke_interval': 600
}

with DAG(
    dag_id='a-nedikov_dag1',
    default_args=DEFAULT_ARGS,
    description='MyfirstDAG',
    schedule_interval='@once',
    max_active_runs=1,
    tags=['a-nedikov'],
) as dag:

    start_task = DummyOperator(task_id='start')

    def log_func(**kwargs):
        logging.info(f'op_args, {{ ds }}: ' + kwargs['ds'])

    second_task = PythonOperator(
    task_id='second_task',
    python_callable=log_func,
    provide_context=True
    )

    end_task = BashOperator(
        task_id='end_task',
        bash_command='echo {{ execution_date }}'
    )

    start_task >> second_task >> end_task