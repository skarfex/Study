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
from e_merkusheva_19_plugins.e_merkusheva_ram_top_location_operator import EMerkusheva19ramtoplocationoperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'e-merkusheva-19',
    'poke_interval': 60
}

with DAG("e-merkusheva-19-lesson-5",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['e-merkusheva-19']
         ) as dag:

    start = DummyOperator(task_id='start')

    print_top_one = EMerkusheva19ramtoplocationoperator(
        task_id='print_top_one',
        top_number=0
    )

    print_top_two = EMerkusheva19ramtoplocationoperator(
        task_id='print_top_two',
        top_number=1
    )

    print_top_three = EMerkusheva19ramtoplocationoperator(
        task_id='print_top_three',
        top_number=2
    )

    start >> print_top_one >> print_top_two >> print_top_three
