"""
Создайте в GreenPlum'е таблицу с названием
"<ваш_логин>_ram_location" с полями id, name, type, dimension, resident_cnt.
С помощью API (https://rickandmortyapi.com/documentation/#location) найдите три локации сериала
"Рик и Морти" с наибольшим количеством резидентов.
Запишите значения соответствующих полей этих трёх локаций в таблицу.
resident_cnt — длина списка в поле residents.
"""

from airflow import DAG
import logging

from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from a_tagirov_15_plugins.atg_ram_top_n_locations_operator import AtgRamTopNLocationsOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-tagirov-15',
    'poke_interval': 600
}

table_name = 'a_tagirov_15_ram_location'
conn_id = 'conn_greenplum_write'

with DAG(
        dag_id='atg_ram_top_locations',
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['atg']
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id=conn_id,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL4 PRIMARY KEY,
                name VARCHAR NOT NULL,
                type VARCHAR NOT NULL,
                dimension VARCHAR NOT NULL,
                resident_cnt INT4 NOT NULL
            );
        """,
        autocommit=True
    )

    truncate_table = PostgresOperator(
        task_id='truncate_table',
        postgres_conn_id=conn_id,
        sql=f"""TRUNCATE TABLE {table_name};""",
        autocommit=True
    )

    load_top_locations_to_gp=AtgRamTopNLocationsOperator(
        task_id='load_top_locations_to_gp',
        conn_id=conn_id,
        table_name=table_name,
        top_n=3,
    )

    start >> create_table >> truncate_table >> load_top_locations_to_gp