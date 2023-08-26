'''
Загружаем данные из API Rick and Morty
'''

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import requests

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from a_tretyakov_19_plugins.tretyakov_atg_ram_top_n_locations_operator import tretyakovLocationsTopOperator

DEFAULT_ARGS = {    
    'start_date': days_ago(2),    
    'owner': 'a-tretjakov-19',
    'poke_interval': 60
}

with DAG("a-tretjakov-19_rick-and-morty",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-tretjakov-19']
         ) as dag:
    
    start = DummyOperator(task_id='start')

    print_top = tretyakovLocationsTopOperator(
        task_id='print_top_one',
        top_number=3
    )
    
    start >> print_top