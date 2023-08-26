"""
Это простейший тестовый даг.
Скопировал у Дины, спасибо :)
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging


from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'v-stratu-12',
    'poke_interval': 600
}

with DAG("v-stratu-12_lsn_3_dag_1",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['v-stratu-12']
          ) as dag:

    def kuku_func():
        logging.info("А вот и не Ky-ky!")


    kuku = PythonOperator(
        task_id='kuku',
        python_callable=kuku_func,
        dag=dag)

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )


    def hello_world_func():
        logging.info("Hello, world!")


    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )

    kuku >> echo_ds >> hello_world

