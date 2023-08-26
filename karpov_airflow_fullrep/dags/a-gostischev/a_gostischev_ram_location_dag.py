"""
DAG API rick and Morty https://rickandmortyapi.com/api/location
Find three local with most county residents
"""

import logging
from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy_operator import DummyOperator
from a_gostischev_plugins.a_gostischev_ram_3 import Top3Locations_N
from airflow.providers.postgres.operators.postgres import PostgresOperator


DEFAULT_ARGS = {
    'owner': 'a-gostischev',
    'start_date': days_ago(2),
    'poke_interval': 30
}
dag = DAG(
    dag_id="a_gostischev_top3_RAM",
    schedule_interval='@hourly',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-gostischev']
)

start = DummyOperator(
    task_id='start',
    dag=dag
)



create_table = f'''
create table if not exists "a_gostischev_ram_location"
(
    system_key int primary key,
    id int,
    name varchar(255),
    type varchar(255),
    dimension varchar(255),
    residents_cnt int
)
DISTRIBUTED BY (system_key)
;
truncate table "a_gostischev_ram_location";
'''

create_or_truncate_tbl = PostgresOperator(
    task_id='create_or_truncate_tbl',
    postgres_conn_id='conn_greenplum_write',
    sql=create_table,
    autocommit=True,
    dag=dag
)

get_top_locations = Top3Locations_N(
    task_id='get_top_locations',
    dag=dag
    )

finish = DummyOperator(
    task_id='finish',
    dag=dag
)


start >> create_or_truncate_tbl >> get_top_locations >> finish
