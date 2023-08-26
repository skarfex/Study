import os
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

#import sys
#sys.path.append('/var/lib/airflow/airflow/airflow.git/plugins/e_rachinskaja_plugins')
from a_trofimov_plugins.a_trofimov_test import Test

DEFAULT_ARGS = {
    'start_date': datetime(2022, 6, 13),
    'owner': 'a.trofimov',
    'poke_interval': 64}

dag = DAG("a-trofimov-dag1",
          schedule_interval='@hourly',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a.trofimov'])

dummy = DummyOperator(
    task_id="dummy",
    dag=dag)

def hello_world_func():
    logging.info("Hello World test dag 1")
    
logging.info(str(os.path.isdir('/var/lib/airflow/airflow/airflow.git/plugins')))
    
logging.info(str(os.path.isdir('/var/lib/airflow/airflow/airflow.git/plugins/e_rachinskaja_plugins')))
    
hello_world = PythonOperator(
    task_id='hello_world',
    python_callable=hello_world_func,
    dag=dag)

dummy >> hello_world

