"""
Создайте в GreenPlum'е таблицу с названием "<ваш_логин>_ram_location" с полями id, name, type, dimension, resident_cnt.
С помощью API (https://rickandmortyapi.com/documentation/#location) найдите три локации сериала "Рик и Морти" с наибольшим количеством резидентов.
Запишите значения соответствующих полей этих трёх локаций в таблицу. resident_cnt — длина списка в поле residents.
"""

import logging
from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from nic_osipov_plugins.nic_osipov_top_3 import select_top_3_locations


DEFAULT_ARGS = {
    'owner': 'nic-osipov',
    'start_date': days_ago(2),
    'poke_interval': 300
}

with DAG(
    dag_id="nic_osipov_lesson_5",
    schedule_interval='@hourly',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['nic-osipov']
) as dag:

    start = DummyOperator(
        task_id='start',
        dag=dag
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        sql=
        '''
        CREATE TABLE IF NOT EXISTS nic_osipov_ram_location
            (
                id int,
                name varchar(255),
                type varchar(255),
                dimension varchar(255),
                residents_cnt int
            )
            DISTRIBUTED BY (id)
            ;
        ''',
        autocommit=True,
        dag=dag
        )

    clear_table = PostgresOperator(
        task_id='clear_table',
        postgres_conn_id='conn_greenplum_write',
        sql='TRUNCATE TABLE nic_osipov_ram_location;'
    )

    select_top_locations = select_top_3_locations(
        task_id='select_top_3_locations',
        dag=dag
        )

    finish = DummyOperator(
        task_id='finish',
        dag=dag
    )


    start >> create_table >> clear_table >> select_top_locations >> finish