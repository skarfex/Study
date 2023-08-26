"""
Даг для задания по ETL - урок 3
создать даг из нескольких тасков, на своё усмотрение:
— DummyOperator
— BashOperator с выводом даты
— PythonOperator с выводом даты
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {'start_date': days_ago(2),
                'owner': 'n-chepurnyh-19',
                'poke_interval': 600}


def hello_world_func():
    logging.info("Start date is {{ ds }}")


with DAG("n-chepurnyh-19_etl_3",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['n-chepurnyh-19']
         ) as dag:
    dummy = DummyOperator(task_id="dummy", dag=dag)
    echo_ds = BashOperator(task_id='echo_ds',
                           bash_command='echo {{ ds }}',
                           dag=dag)
    python_op = PythonOperator(task_id='python_op',
                               python_callable=hello_world_func,
                               dag=dag)
dummy >> [echo_ds, python_op]
