"""
Первый написанный dag
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'd-kuzmin-11',
    'poke_interval': 300
}

dag = DAG("d-kuzmin-11_first_dag",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['d-kuzmin-11']
          )

dummy = DummyOperator(task_id="dummy", dag=dag)

echo_ds = BashOperator(
    task_id='echo_ds',
    bash_command='echo {{ ds }}',
    dag=dag
)


def hello_world_func():
    logging.info("Hello World!")


hello_world = PythonOperator(
    task_id='hello_world',
    python_callable=hello_world_func,
    dag=dag
)

dummy >> [echo_ds, hello_world]
