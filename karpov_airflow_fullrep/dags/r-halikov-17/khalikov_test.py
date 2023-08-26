from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'khalikov',
    'poke_interval': 600
}

with DAG(
    "khalikov_test",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['khalikov_test'],
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id="echo_ds",
        bash_command='echo {{ds}}',
        dag=dag
    )

    def hello_world_func():
        logging.info("Hello Khalikov Ruslan")
        return

    hello_world = PythonOperator(
        task_id="hello_world_khalikov",
        python_callable=hello_world_func,
        dag=dag
    )

    dummy >> [echo_ds, hello_world]