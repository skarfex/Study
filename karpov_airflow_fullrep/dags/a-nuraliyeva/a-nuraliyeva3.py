"""
это DAG состоящий из:
DummyOperator
BashOperator с выводом даты
PythonOperator с выводом даты

"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a-nuraliyeva',
    'poke_interval': 600
}

with DAG("a-nuraliyeva3",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-nuraliyeva']
          ) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def print_execution_date(ds, **kwargs):
        print(ds)

    print_date = PythonOperator(
        task_id='print_date',
        python_callable=print_execution_date,
        dag=dag
    )

    dummy >> [echo_ds, print_date]

