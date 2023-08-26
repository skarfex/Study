"""
Загружаем данные о локациях из API Рика и Морти
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import requests


from airflow.exceptions import AirflowException
from d_bokarev_plugins.d_bokarev_ram_operator import DBokarevRickMortyOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'd-bokarev',
    'poke_interval': 600
}


with DAG("d_bokarev_ram_location",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['d-bokarev']
         ) as dag:

    print_location_dict = DBokarevRickMortyOperator(
        task_id='print_location_dict'
    )


    print_location_dict