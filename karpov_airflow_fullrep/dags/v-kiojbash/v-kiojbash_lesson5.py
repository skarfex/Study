'''
Создайте в GreenPlum'е таблицу с названием "<ваш_логин>_ram_location"
с полями id, name, type, dimension, resident_cnt.
! Обратите внимание: ваш логин в LMS нужно использовать, заменив дефис на нижнее подчёркивание

С помощью API (https://rickandmortyapi.com/documentation/#location)
найдите три локации сериала "Рик и Морти" с наибольшим количеством резидентов.

Запишите значения соответствующих полей этих трёх локаций в таблицу. resident_cnt — длина списка в поле residents.
'''

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import requests

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'v-kiojbash',
    'poke_interval': 600
}

pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

def create_table():
    CREATE_TABLE_SQL_STATEMENT = """CREATE TABLE IF NOT EXISTS v_kiojbash_ram_location (
                                        id int, 
                                        name varchar(100), 
                                        type varchar(100), 
                                        dimension varchar(100),
                                        resident_cnt int)"""
    pg_hook.run(CREATE_TABLE_SQL_STATEMENT, False)
    logging.info('SQL STATEMENT EXECUTION COMPLETED SUCCESSFULLY')

def find_top3_locations(**kwargs):
    ti = kwargs['ti']
    ans = requests.get('https://rickandmortyapi.com/api/location')
    location_list = []

    for item in ans.json()['results']:
        location_dict = {}
        location_dict['id'] = item['id']
        location_dict['name'] = item['name']
        location_dict['type'] = item['type']
        location_dict['dimension'] = item['dimension']
        location_dict['resident_cnt'] = len(item['residents'])
        location_list.append(location_dict)

    ti.xcom_push('location_list', sorted([item for item in location_list], key=lambda x: x['resident_cnt'], reverse=True)[:3])

    logging.info('TOP-3 LOCATIONS: {}'.format(sorted([item for item in location_list], key=lambda x: x['resident_cnt'], reverse=True)[:3]))

def write_data_to_gp(ti):
    location_list = ti.xcom_pull(task_ids='get_locations', key='location_list')
    logging.info('XCOM_PULL PARAMETER ACCEPTED: {}'.format(location_list))
    for item in location_list:
        pg_hook.run("INSERT INTO v_kiojbash_ram_location VALUES({}, '{}', '{}', '{}', {})".format(item['id'],
                                                                                                 item['name'],
                                                                                                 item['type'],
                                                                                                 item['dimension'],
                                                                                                 item['resident_cnt']), False)
    logging.info('INSERT STATEMENT COMPLETED SUCCESSFULLY')

with DAG('v-kiojbash-lesson5',
         default_args=DEFAULT_ARGS,
         schedule_interval='@daily',
         max_active_runs=1,
         tags=['v-kiojbash']
         ) as dag:
    create_table = PythonOperator(
        task_id='create_table',
        python_callable=create_table
    )

    get_locations = PythonOperator(
        task_id='get_locations',
        python_callable=find_top3_locations
    )

    write_data = PythonOperator(
        task_id='write_data',
        python_callable=write_data_to_gp
    )

    create_table >> get_locations >> write_data