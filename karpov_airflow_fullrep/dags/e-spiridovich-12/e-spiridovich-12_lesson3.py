from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(7),
    'owner': 'e-spiridovich-12',
    'poke_interval': 600
}

with DAG ("e-spiridovich-12_lesson3",
          schedule_interval='@hourly',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['e-spiridovich-12']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    bash = BashOperator(
        task_id='bash_string',
        bash_command='echo "This is my bash dag"'
    )

    def python_print():
        print("This is my PYTHON dag")

    python = PythonOperator(
        task_id='python_string',
        python_callable=python_print
    )

    dummy >> [python, bash]
