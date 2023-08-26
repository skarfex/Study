"""
Тестовый даг
"""
from airflow import DAG
import logging
import requests
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from m_rasskazov_20_plugins.m_rasskazov_20_operator_ram_char import MR20RamSpeciesCountOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022,3,1),
    'owner': 'm-rasskazov-20',
    'poke_interval': 600
}

with DAG("m-rasskazov-20-l5",
    schedule_interval='@once',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['m-rasskazov-20-l5_RAM_char']
) as dag:

    print_alien_count = MR20RamSpeciesCountOperator(
        task_id='print_alien_count',
        species_type='Alien'
    )

print_alien_count