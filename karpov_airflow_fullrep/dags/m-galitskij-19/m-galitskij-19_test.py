"""
Тестовый доработанный даг Галицкий М
"""
import logging
from datetime import datetime, timedelta

from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {    
    'start_date': days_ago(3),    
    'owner': 'm-galitskij-19',
    'poke_interval': 600
}

dag = DAG(
    'm-galitskij-19_test_dag',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['m-galitskij-19'],
    catchup=True
)

end = DummyOperator(
    task_id = 'end'
)

def hello_world_func():
    logging.info("Hello World!")

hello_world = PythonOperator(
    task_id='hello_world',
    python_callable=hello_world_func,
    dag=dag
)

hello_world >> end