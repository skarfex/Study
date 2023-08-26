"""
Load top-3 locations from Rick and Morty API
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import requests
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook

from airflow.hooks.postgres_hook import PostgresHook
from m_grigoreva_16_plugins.m_grigoreva_16_locations_operator import MGrigorevaRamLocationsOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm.grigoreva-16',
    'poke_interval': 600
}

with DAG("m.grigoreva-16_task_5",
    schedule_interval=None,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['m.grigoreva-16_tag']) as dag:

    start = DummyOperator(task_id='start')

    def insert_data_to_db(task_instance):
        data = task_instance.xcom_pull(task_ids='get_top_3_locations')
        data_formated = [f"({x['id']}, '{x['name']}', '{x['type']}', '{x['dimension']}', {x['resident_cnt']})" for x
                             in data]
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

        creating_table = """
                        CREATE TABLE IF NOT EXISTS m_grigoreva_16_ram_location (
                                id SERIAL4 PRIMARY KEY,
                                name VARCHAR NOT NULL,
                                type VARCHAR NOT NULL,
                                dimension VARCHAR NOT NULL,
                                resident_cnt INT4 NOT NULL)
                    """
        truncating_table = "TRUNCATE TABLE m_grigoreva_16_ram_location"
        inserting_values = f"INSERT INTO m_grigoreva_16_ram_location VALUES {','.join(data_formated)}"

        pg_hook.run(creating_table, False)
        pg_hook.run(truncating_table, False)
        pg_hook.run(inserting_values, False)

    get_top_3_locations = MGrigorevaRamLocationsOperator(
        task_id='get_top_3_locations'
    )

    insert_locations_to_gp = PythonOperator(
        task_id='insert_locations_to_gp',
        python_callable=insert_data_to_db
    )

    start >> get_top_3_locations >> insert_locations_to_gp


