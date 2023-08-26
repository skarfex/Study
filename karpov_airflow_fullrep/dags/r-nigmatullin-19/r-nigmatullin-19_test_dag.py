"""
Это тестовый даг.
Он состоит из пустого оператора,
баш-оператора (выводит execution_date),
одного питон-оператора (выводит execution_date)
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from textwrap import dedent

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'r-nigmatullin-19',
    'poke_interval': 600
}

with DAG("rmnigm_test_dag",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['r-nigmatullin-19']) as dag:

    dummy = DummyOperator(task_id='dummy')
    
    echo_ds_bash = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}'
    )

    text_date = dedent("ds: {{ ds }}")
    
    def first_func(text_date):
        logging.info(text_date)
    
    
    first_task = PythonOperator(
        task_id='python_log_ds',
        python_callable=first_func,
        op_args=[text_date]
    )
    
    dummy >> [echo_ds_bash, first_task]
