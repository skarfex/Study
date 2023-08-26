"""
Dag для урока 5
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import requests

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from dina_plugins.dina_ram_species_count_operator import DinaRamSpeciesCountOperator
from dina_plugins.dina_ram_dead_or_alive_operator import DinaRamDeadOrAliveCountOperator
from airflow.hooks.postgres_hook import PostgresHook


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v-kotezhekov-20',
    'poke_interval': 600
}

with DAG("v_kotezhekov_20_ram_location",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['v-kotezhekov-20']
         ) as dag:

    start = DummyOperator(task_id='start')

    def load_ram_func():
        """
        Loading ram locations from API
        """
        ram_char_url = 'https://rickandmortyapi.com/api/location'
        r = requests.get(ram_char_url)
        get_data_from_json (r.json().get('results'))

    def get_data_from_json (result_json):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run ('TRUNCATE TABLE public.v_kotezhekov_20_ram_location')
        for one_char in result_json:
            id = one_char.get('id')
            name = one_char.get('name')
            type = one_char.get('type')
            dimension = one_char.get('dimension')
            residents = one_char.get('residents')
            resident_cnt = len (residents)
            logging.info(f'     id = {id}')
            pg_hook.run('INSERT INTO public.v_kotezhekov_20_ram_location (id, name, type, dimension, resident_cnt) VALUES (%s,%s,%s,%s,%s)', parameters=(id, name, type, dimension, resident_cnt))
            # cursor.execute('SELECT heading FROM articles WHERE id = %s', (weekday,)) # исполняем sql

    def get_top_locations ():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run('DELETE FROM public.v_kotezhekov_20_ram_location WHERE id NOT IN (SELECT id FROM public.v_kotezhekov_20_ram_location r ORDER  BY r.resident_cnt desc LIMIT 3);')


    save_to_database = PythonOperator(
        task_id='save_to_database',
        python_callable=load_ram_func
    )

    find_top_locations = PythonOperator (
        task_id = 'find_top_locations',
        python_callable = get_top_locations
    )
    start >> save_to_database >> find_top_locations
