"""
Airflow DAG. Lesson 3.
DAG with
- DummyOperator (deprecated!);
- BashOperator with console output;
- PythonOperator with console output;
- some simple logic.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime, timedelta
import sys

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

now = datetime.now()

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 's-gerasimov-17',
    'poke_interval': 600,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(seconds=10)
}

with DAG("s-gerasimov-17-lesson3",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['s-gerasimov-17']
         ) as dag:
    dummy_start = DummyOperator(task_id="dummy_start")

    bash_hello_world = BashOperator(
        task_id='echo_ds',
        bash_command='echo "Hello World! Today is {{ ds }}" && echo "System is " && cat /etc/*release',
        dag=dag
    )


    def hello_world_func():
        logging.info("Hello World! Today is ", now.strftime("%Y-%m-%d)"), " Python version is", sys.version)


    python_operator = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )

    dummy_end = DummyOperator(task_id="dummy_end")

    dummy_start >> bash_hello_world >> python_operator >> dummy_end
