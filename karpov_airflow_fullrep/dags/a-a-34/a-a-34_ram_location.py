"""
Getting location data from Rick and Morty API and inserting to greenplum
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import json
import requests

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-a-34',
    'poke_interval': 600
}

with DAG('a-a-34_ram_location',
    schedule_interval = '@daily',
    default_args = DEFAULT_ARGS,
    max_active_runs = 1,
    tags = ['a-a-34']
) as dag:

    def load_location_from_api_func():
        location_data = []

        ram_locations = requests.get('https://rickandmortyapi.com/api/location')
        locations = json.loads(ram_locations.text)

        for location in locations['results']:
            location_data.append((location['id'],
                                  location['name'],
                                  location['type'],
                                  location['dimension'],
                                  len(location['residents'])))
        sorted_location_data = sorted(location_data, key=lambda x: x[-1], reverse=True)[:3]
        result = ','.join(map(str, sorted_location_data))
        return result

    load_location_from_api = PythonOperator(
        task_id='load_location_from_api',
        python_callable=load_location_from_api_func,
        dag=dag
    )

    def create_greenplum_table_func():
        request = '''
        CREATE TABLE if not exists public."a-a-34_ram_location" (
            id int4 NOT NULL,
            "name" varchar(256) NULL,
            "type" varchar(256) NULL,
            dimension varchar(256) NULL,
            resident_cnt int4 NULL,
            CONSTRAINT "a-a-34_ram_location_pkey" PRIMARY KEY (id)
        )
        DISTRIBUTED BY (id);'''
        pg_hook = PostgresHook(postgres_conn_id="conn_greenplum_write")
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(request)
        connection.commit()
        cursor.close()
        connection.close()

    create_greenplum_table = PythonOperator(
        task_id='create_greenplum_table',
        python_callable=create_greenplum_table_func,
        dag=dag
    )

    def truncate_greenplum_table_func():
        request = '''TRUNCATE TABLE public."a-a-34_ram_location"'''
        pg_hook = PostgresHook(postgres_conn_id="conn_greenplum_write")
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(request)
        connection.commit()
        cursor.close()
        connection.close()

    truncate_greenplum_table = PythonOperator(
        task_id='truncate_greenplum_table',
        python_callable=truncate_greenplum_table_func,
        dag=dag
    )

    insert_into_greenplum = PostgresOperator(
        task_id='insert_into_greenplum',
        postgres_conn_id='conn_greenplum_write',
        sql='INSERT INTO public."a-a-34_ram_location" VALUES {{ ti.xcom_pull(task_ids="load_location_from_api") }}',
        autocommit=True
    )

    load_location_from_api >> create_greenplum_table >> truncate_greenplum_table >> insert_into_greenplum
