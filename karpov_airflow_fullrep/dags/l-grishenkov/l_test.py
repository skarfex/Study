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
    'owner': 'l',
    'poke_interval': 600
}

with DAG("l_test",
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['l']
        ) as dag:

    dummy = DummyOperator(task_id='dummy')

    echo_l = BashOperator(
        task_id='echo_l',
        bash_command='echo {{ l }}',
        dag=dag
    )


    def l_test_func():
        logging.info("l test")


    l_test = PythonOperator(
        task_id='task_2',
        python_callable=l_test_func,
        dag=dag
    )

    dummy >> [echo_l, l_test]