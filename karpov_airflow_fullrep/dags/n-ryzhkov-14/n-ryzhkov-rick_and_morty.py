"""
DAG for find in API rick and Morty three local with most county residents
"""

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy_operator import DummyOperator
from n_ryzhkov_14_plugins.n_ryzhkov_14_rick_and_morty_operator import nryzhkovTop3Locations
from airflow.providers.postgres.operators.postgres import PostgresOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'n-ryzhkov-14',
    'poke_interval': 600
}

dag = DAG(
    dag_id="n_ryzhkov_14_rick_and_morty",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['n-ryzhkov-14']
)

start = DummyOperator(
    task_id='start',
    dag=dag
)

create_table = f'''
create table if not exists "n_ryzhkov_14_ram_location"
(
    system_key int primary key,
    id int,
    name varchar(256),
    type varchar(256),
    dimension varchar(256),
    residents_cnt int
)
DISTRIBUTED BY (system_key);

truncate table "n_ryzhkov_14_ram_location";
'''

create_or_truncate_table = PostgresOperator(
    task_id='create_or_truncate_table',
    postgres_conn_id='conn_greenplum_write',
    sql=create_table,
    autocommit=True,
    dag=dag
)

get_top_locations = nryzhkovTop3Locations(
    task_id='get_top_locations',
    dag=dag
    )

finish = DummyOperator(
    task_id='finish',
    dag=dag
)


start >> create_or_truncate_table >> get_top_locations >> finish
