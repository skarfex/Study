"""
    Забирает из https://rickandmortyapi.com/api/location данные о локации, считает количество резидентов и помещает в GreenPlum
    a_ibragimbegov_ram_location
"""

import logging
from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy_operator import DummyOperator
from a_ibragimbegov_plugins.rickandmorty_top3_locations_operator import Top3Locations_N
from airflow.providers.postgres.operators.postgres import PostgresOperator


DEFAULT_ARGS = {
    'owner': 'a-ibragimbegov',
    'start_date': days_ago(2),
    'poke_interval': 30
}
dag = DAG(
    dag_id="a_ibragimbegov_ram_dag",
    schedule_interval='@hourly',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-ibragimbegov']
)

start = DummyOperator(
    task_id='start',
    dag=dag
)

# (1, '20','Earth (Replacement Dimension)','Planet','Replacement Dimension','230')
# (2, '3','Citadel of Ricks','Space station','unknown','101')

create_table = f'''
drop table if exists "a_ibragimbegov_ram_location";
create table if not exists "a_ibragimbegov_ram_location"
(
    system_key int primary key,
    id int,
    name varchar(255),
    type varchar(255),
    dimension varchar(255),
    residents_cnt int
)
DISTRIBUTED BY (system_key);
truncate table "a_ibragimbegov_ram_location";
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