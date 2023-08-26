"""
Это тестовый даг
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import  PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'i-romerorejes-11',
    'poke_interval': 600
}

with DAG("i-romerorejes-11_test",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1, # это означает что в единственный момент времени запущен только один даг
          tags=['i_romerorejes_11']
          ) as dag:

        dummy = DummyOperator(task_id = 'dummy')

        echo_ilacai = BashOperator(
            task_id='echo_ilacai',
            bash_command='date +%Y-%m-%d',
            dag=dag
        )


        def hello_world_func():
            logging.info("Hello World!")


        hello_world = PythonOperator(
            task_id='hello_world',
            python_callable=hello_world_func,
            dag=dag
        )

        dummy >> [echo_ilacai, hello_world]



