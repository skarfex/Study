
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-grishin',
    'poke_interval': 600
}

with DAG("a-grishin-simple-dag",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-grishin']
          ) as dag:

    dummy = DummyOperator(task_id="dummy", dag=dag)

    echo_to_log = BashOperator(
        task_id='echo_to_log',
        bash_command='echo -n "finally we did it"',
        dag=dag
    )


    def hello_world_func():
        logging.info("Hello World")


    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )


    dummy >> [echo_to_log, hello_world]
