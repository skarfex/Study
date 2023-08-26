"""
Тестовый DAG
"""


from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    "start_date": days_ago(2),
    'owner': 'n-ryzhkov-14',
    'poke_interval': 600
}
with DAG("n-ryzhkov-14_test",
         schedule_interval="@daily",
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['n-ryzhkov-14_test']
         ) as dag:


    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )
    def some_text_func():
        logging.info("some text")

    some_text = PythonOperator(
            task_id='hello_world',
            python_callable=some_text_func,
            dag=dag
        )
dummy >> [echo_ds, some_text]

