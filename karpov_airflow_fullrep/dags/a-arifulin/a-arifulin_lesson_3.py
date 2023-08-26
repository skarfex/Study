"""
Description

"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-arifulin',
    'poke_interval': 600
}

with DAG("a-arifulin_test",
    schedule_interval = '@daily',
    default_args = DEFAULT_ARGS,
    max_active_runs = 1,
    tags = ['a-arifulin']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id = 'echo_ds',
        bash_command = 'echo {{ ds }}',
        dag = dag
    )

    def print_date(ds, **kwargs):
        print(ds)
        return 'Successful!'

    python_date = PythonOperator(
        task_id = 'python_date',
        python_callable = print_date,
        provide_context = True,
        dag = dag
    )

    dummy >> echo_ds >> python_date
