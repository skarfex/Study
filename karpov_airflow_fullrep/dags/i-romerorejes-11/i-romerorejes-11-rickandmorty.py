"""
Задание

Создайте в GreenPlum'е таблицу с названием "<ваш_логин>_ram_location" с полями id, name, type, dimension, resident_cnt.
С помощью API (https://rickandmortyapi.com/documentation/#location) найдите три локации сериала "Рик и Морти" с наибольшим количеством резидентов.
Запишите значения соответствующих полей этих трёх локаций в таблицу. resident_cnt — длина списка в поле residents.

"""

import requests
from datetime import datetime, timedelta
from airflow import DAG
import logging
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from i_romerorejes_11_plugins.i_romerorejes_11_operator import TopLocations

conn_id = 'conn_greenplum_write'
table_nm = 'i_romerorejes_11_rickandmorty_top_locations'

DEFAULT_ARGS = {
    'start_date': datetime(2022, 6, 6),
    'owner': 'i-romerorejes-11',
    'poke_interval': 600
}

dag = DAG("i-romeroreyes-11-rickandmorty",
          schedule_interval='0 10 * * *',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          catchup=True,
          tags=['i_romerorejes_11']
          )

create_and_truncate_table = PostgresOperator(
    task_id='create_and_clear_table',
    postgres_conn_id=conn_id,
    sql=f"""
    CREATE TABLE IF NOT EXISTS {table_nm} (
        "id" int, 
        name varchar, 
        type varchar, 
        dimension varchar, 
        resident_cnt int);
    TRUNCATE TABLE {table_nm};
    """,
    dag=dag
)

get_top3_location = TopLocations(
    task_id='get_top3_location',
    dag=dag
)


remove_ram_csv = BashOperator(
    task_id='remove_ram_csv',
    bash_command=f'rm -f /tmp/i-romeroreyes-11_top_locations.csv',
    dag=dag
)

create_and_truncate_table >> get_top3_location >> remove_ram_csv