import datetime
import logging
import requests

from airflow import DAG
from airflow.utils.dates import days_ago
from i_ilhman_plugins.i_ilhman_m3l5_operator import IlhmanRamOperator

from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'i_ilhman'
}

with DAG("ilhman_ram_dag_v4",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['i-ilhman']
) as dag:
    start = DummyOperator(task_id="start")

    create_table = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id='conn_greenplum_write',
        sql = f'''
        CREATE TABLE IF NOT EXISTS ilhman_ram_location (
            id integer primary key,
            name text,
            type text,
            dimension text,
            resident_cnt integer
        )
        ''')

    get_top3_location = IlhmanRamOperator(
        task_id='get_top3_location'
    )

    start >> create_table >> get_top3_location

