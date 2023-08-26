"""
Это простейший даг.

"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'svreutov',
    'poke_interval': 600
}

with DAG("svreutov_test",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['svreutov']
          ) as dag:

    dummy = DummyOperator(
        task_id='dummy'
    )

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )


    def hello_world_func():
        logging.info("Hello world")


    hello_world = PythonOperator(
        task_id='first_task',
        python_callable=hello_world_func,
        dag=dag
    )


    dummy >> [echo_ds >> hello_world]

    dag.doc_md = __doc__

    dummy.doc_md = """ишет в лог dummy"""
    echo_ds.doc_md = """Пишет в лог echo_ds"""
    hello_world.doc_md = """Пишет в лог hello_world"""
