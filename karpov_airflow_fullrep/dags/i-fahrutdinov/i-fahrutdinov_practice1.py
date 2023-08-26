"""
Тестовый даг для практики 1
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(5),
    'owner': 'i-fahrutdinov',
    'poke_interval': 600
}

with DAG("i-fahrutdinov_practice1",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['i-fahrutdinov']
         ) as dag:

    dummy = DummyOperator(task_id="start")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds_nodash }}',
        dag=dag
    )

    def hello_world(**context):
        ed = context['execution_date']
        conf = context['conf']
        logging.info(f"Hello World! \nMy execution date is | {ed} | with config: {conf}.")

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world,
        dag=dag
    )

    dummy >> echo_ds >> hello_world
