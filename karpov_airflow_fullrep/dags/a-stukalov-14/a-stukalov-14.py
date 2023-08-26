import requests
import json
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash_operator import BashOperator
from  datetime import datetime
from a_stukalov_14_plugins.rick_morty_operator import RickMortyLocOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'owner': 'as',
    'poke_interval': 600
}
with DAG("a-stukalov-14",
    schedule_interval='5 4 * * *',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['stukalov']
) as dag:
     top_3_location = RickMortyLocOperator(
          task_id='print_alien_count',
     )
top_3_location
