
"""
Пример из лекции 5 - загружаем данные из API Рика и Морти
"""

from airflow import DAG
from airflow.utils.dates import days_ago
#import logging
#import requests

from airflow.operators.dummy import DummyOperator
#from airflow.operators.python_operator import PythonOperator
#from airflow.exceptions import AirflowException

from e_ponomareva_plugins.e_ponomareva_ram_species_count_operator import eponomarevaRamSpeciesCountOperator
from e_ponomareva_plugins.e_ponomareva_ram_dead_or_alive_operator import eponomarevaRamDeadOrAliveCountOperator
from e_ponomareva_plugins.e_ponomareva_random_sensor import eponomarevaRandomSensor



DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'e-ponomareva',
    'poke_interval': 600
}

with DAG("e-ponomareva_load_ram_test",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['e-ponomareva']
         ) as dag:

    start = DummyOperator(task_id='start')

    random_wait = eponomarevaRandomSensor(
        task_id='random_wait',
        mode='reschedule',
        range_number=5
    )

    print_alien_count = eponomarevaRamSpeciesCountOperator(
        task_id='print_alien_count',
        species_type='Alien'
    )

    print_dead_count = eponomarevaRamDeadOrAliveCountOperator(
        task_id='print_dead_count',
        dead_or_alive='Dead'
    )

    start >> random_wait >> [print_alien_count, print_dead_count]
