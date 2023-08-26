"""
Автоматизация ETl-процессов
5 Урок
Домашняя работа
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import logging
import pendulum
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from e_zverkov_plugins.e_zverkov_ram_location import (EZVERKOVRickAndMortyTopLocationOperator, )

DEFAULT_ARGS = {
    'owner': 'e-zverkov',
    'start_date': days_ago(3),
    'poke_interval': 600
}

with DAG("e-zverkov_dag_3",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=3,
         tags=['e-zverkov']
) as dag:

    create_table = PostgresOperator(
    task_id="create_table",
    postgres_conn_id='conn_greenplum_write',
    sql='''
            create table if not exists public.e_zverkov_ram_location (
            id            integer,
            name          text,
            type          text,
            dimension     text,
            resident_cnt  integer
            );
        ''',
    autocommit=True,
    dag=dag
    )

    get_top_locations = EZVERKOVRickAndMortyTopLocationOperator(task_id='get_top_locations', dag=dag)
    create_table >> get_top_locations
