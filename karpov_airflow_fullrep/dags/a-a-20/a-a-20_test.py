
"""
Тестовый даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-a-20',
    'poke_interval': 600
}

with DAG('a-a-20_test',
    schedule_interval='0 0 * * MON,TUE,WED,THU,FRI,SAT',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-a-20']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def PythonOperator_func():
       logging.info(str(datetime.datetime.now()))

    echo_ds_PythonOperator = PythonOperator(
        task_id='echo_ds_PythonOperator',
        python_callable=PythonOperator_func,
        dag=dag
    )

    dummy >> [echo_ds, echo_ds_PythonOperator]
