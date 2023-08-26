import datetime
import logging
import requests

from airflow import DAG
from airflow.utils.dates import days_ago
from il_kuznetsov_plugins.il_kuznetsov_ram_plugin import IlKuznetsovOperator

from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

DEFAULT_ARGS = {
    'start_date': days_ago(0),
    'owner': 'il-kuznetsov'
}

with DAG("il-kuznetsov_ram_dag",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['il-kuznetsov']
) as dag:
    start = DummyOperator(task_id="start")

    create_table = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id='conn_greenplum_write',
        sql = f'''
        CREATE TABLE IF NOT EXISTS il_kuznetsov_ram_location (
            id integer primary key,
            name text,
            type text,
            dimension text,
            resident_cnt integer
        )
        ''')

    get_top3_location = IlKuznetsovOperator(
        task_id='get_top3_location'
    )

    start >> create_table >> get_top3_location
            
