
import os
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 6, 13),
    'owner': 'a.trofimov',
    'poke_interval': 64}

dag = DAG("a-trofimov-dag2",
          schedule_interval='@hourly',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a.trofimov'])

dummy = DummyOperator(
    task_id="dummy",
    dag=dag)

def hello_world_func():
    logging.info("Hello World 2 2 2 2 2")

hello_world = PythonOperator(
    task_id='hello_world',
    python_callable=hello_world_func,
    dag=dag)

dummy >> hello_world
