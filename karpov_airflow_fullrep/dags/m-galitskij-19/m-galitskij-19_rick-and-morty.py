'''
Загружаем данные из API Rick and Morty
'''

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import requests

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from m_galitskij_19_plugins.m_galitskij_19_ram_res_count_operator import Mgalitskij19RamResCountOperator

DEFAULT_ARGS = {    
    'start_date': days_ago(2),    
    'owner': 'm-galitskij-19',
    'poke_interval': 60
}

with DAG("m-galitskij-19_rick-and-morty",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['m-galitskij-19']
         ) as dag:
    
    start = DummyOperator(task_id='start')

    print_top_one = Mgalitskij19RamResCountOperator(
        task_id='print_top_one',
        top_number=0
    )

    print_top_two = Mgalitskij19RamResCountOperator(
        task_id='print_top_two',
        top_number=1
    )

    print_top_three = Mgalitskij19RamResCountOperator(
        task_id='print_top_three',
        top_number=2
    )

    start >> print_top_one >> print_top_two >> print_top_three
