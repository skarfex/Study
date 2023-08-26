from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import datetime

from airflow.hooks.postgres_hook import PostgresHook
from m_borodastov_plugins.m_borodastov_lesson5_operator import MBRamTopLocationsOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator


DEFAULT_ARGS = {
    'owner': 'm-borodastov',
    'start_date': days_ago(2),
    'poke_interval': 600
}

with DAG("m-borodastov_lesson5_ram",
         schedule_interval='@once',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['m-borodastov']
         ) as dag:

    start = DummyOperator(task_id="start")

    get_top_ram_locations = MBRamTopLocationsOperator(
        task_id='get_top_ram_locations'
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        sql="""
            create table if not exists public.m_borodastov_ram_location (
                id integer unique not null,
                name text null,
                type text null,
                dimension text null,
                resident_cnt integer,
                unique(id, name, type, dimension, resident_cnt)) 
                distributed by (id);
            """
    )

    start >> create_table >> get_top_ram_locations