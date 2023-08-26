"""
j-ibragimov-15 ram lesson
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import datetime

from airflow.hooks.postgres_hook import PostgresHook
from j_ibragimov_15_plugins.ram_location import YIRamTopLocationsOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator


DEFAULT_ARGS = {
    'owner': 'j-ibragimov-15',
    'start_date': days_ago(0),
    'poke_interval': 600  
}

with DAG("yi_ram_lesson",
    schedule_interval='@once',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['j-ibragimov-15']
) as dag:
           
    top_ram_locations = YIRamTopLocationsOperator(
        task_id='top_ram_locations'
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        sql="""
            create table if not exists public.j_ibragimov_15_ram_location (
                id integer unique not null,
                name text null,
                type text null,
                dimension text null,
                resident_cnt integer,
                unique(id, name, type, dimension, resident_cnt)) 
                distributed by (id);
            """   
    )

    create_table >> top_ram_locations