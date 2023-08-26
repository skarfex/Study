"""
Топ-3 локации: выгрузка данных из API Рика и Морти.
"""
import logging
import requests

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.postgres_operator import PostgresOperator
from ang_semenova_plugins.ang_semenova_ram_operator import AngSemenovaRamOperator



DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'ang-semenova',
    'poke_interval': 600
}


with DAG("ang-semenova-load-ram",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['ang-semenova']
         ) as dag:
    
    check_table = PostgresOperator(
        task_id = 'check_table',
        postgres_conn_id = 'conn_greenplum_write',
        sql = '/check_table.sql'
    )

    truncate_table = PostgresOperator(
        task_id = 'truncate_table',
        postgres_conn_id = 'conn_greenplum_write',
        sql = 'TRUNCATE TABLE ang_semenova_ram_location;'
    )

    top_3_locations = AngSemenovaRamOperator(
        task_id = 'top_3_locations'
    )

    check_table >> truncate_table >> top_3_locations