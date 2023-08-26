'''
API Rick and Morty
'''

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import requests
import m_goldin_20_plugins
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from m_goldin_20_plugins.m_goldin_20_ram_count_operator import MGOLDIN20RAMCOUNTOPERATOR

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm-goldin-20',
    'poke_interval': 60
}

with DAG("m-goldin_20_rick-and-morty",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['m-goldin-20']
         ) as dag:

    start = DummyOperator(task_id='start')

    print_top1 = MGOLDIN20RAMCOUNTOPERATOR(
        task_id='print_top_0',
        top_number=0
    )

    print_top2 = MGOLDIN20RAMCOUNTOPERATOR(
        task_id='print_top_2',
        top_number=1
    )

    print_top3 = MGOLDIN20RAMCOUNTOPERATOR(
        task_id='print_top_3',
        top_number=2
    )


    start >> print_top1
    start >> print_top2
    start >> print_top3
