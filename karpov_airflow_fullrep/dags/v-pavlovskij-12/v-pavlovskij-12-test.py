from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v-pavlovskij-12',
    'poke_interval': 600
}

with DAG(
    'v-pavlovskij-12-simple-dag',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['v-pavlovskij-12']
) as dag: 

    dummy = DummyOperator(
        task_id='dummy',
    )

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
    )

    def hw_func():
        logging.info('Hello world')
    
    hw_task = PythonOperator(
        task_id='hw_task',
        python_callable=hw_func
    )

    dummy >> [echo_ds, hw_task]


