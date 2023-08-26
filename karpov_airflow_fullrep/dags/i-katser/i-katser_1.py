
"""
Тестовый даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'i-katser',
    'poke_interval': 600
}

with DAG("DAG_i-katser",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['i-katser']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    date_bash = BashOperator(
        task_id='date_bash',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def date_func(**context):
        dat = context['execution_date']
        logging.info(f'date: {dat}')

    date_python = PythonOperator(
        task_id='date_python',
        python_callable=date_func,
        dag=dag
    )

    dummy >> [date_bash, date_python]


