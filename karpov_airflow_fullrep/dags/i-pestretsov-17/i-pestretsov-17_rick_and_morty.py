"""
rick and morty dag
"""

from datetime import datetime

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator

from i_pestretsov_17_plugins.get_locations_operator import GetLocationsInfoOperator

DEFAULT_ARGS = {
    'schedule_interval': '@once',
    'owner': 'i-pestretsov-17',
    'poke_interval': 600,
    'start_date': datetime(2023, 2, 14)
}

with DAG("i-pestretsov-17_ram",
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['i-pestretsov']
         ) as dag:
    start = DummyOperator(task_id="dummy")

    table_name = 'i-pestretsov-17_ram_location'
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

    get_data = GetLocationsInfoOperator(
        task_id='get_data',
        dag=dag,
        table=table_name
    )

    start >> get_data
