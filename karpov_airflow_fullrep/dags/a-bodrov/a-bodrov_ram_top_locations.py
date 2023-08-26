"""
Getiing locations from API https://rickandmortyapi.com/api/
And sending top3 ones by residents count to GreenPlum DB
"""

from airflow import DAG
from airflow.utils.dates import days_ago

import logging
import json

from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

from a_bodrov_plugins.abodrov_ram_operator import ABRRickMortyAPILocationsOperator



DEFAULT_ARGS = {
    'owner': 'a-bodrov',
    'poke_interval': 600,
    'wait_for_dowmstream': True

}

TARGET_TABLE = 'a_bodrov_ram_location'

with DAG(
        "a-bodrov_ram_top3_locations",
        schedule_interval='@daily',
        start_date=datetime(2022, 6, 15),
        end_date=datetime(2022, 6, 16),
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['a-bodrov']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    get_top3_ram_locations = ABRRickMortyAPILocationsOperator(
        task_id='get_top3_ram_locations'
    )

    def truncate_ram_table_func():
        pg_hook = PostgresHook('conn_greenplum_write') ## TODO
        sql = "TRUNCATE TABLE {table};".format(table=TARGET_TABLE)
        pg_hook.run(sql)

    truncate_ram_table = PythonOperator(
        task_id='truncate_ram_table',
        python_callable=truncate_ram_table_func
    )

    def upload_ram_locations_to_gp_func(**kwargs):
        pg_hook = PostgresHook('conn_greenplum_write') ## TODO
        sql = ""
        locations = json.loads(kwargs['ti'].xcom_pull(task_ids='get_top3_ram_locations', key='return_value'))
        for location in locations:
            sql += "INSERT INTO {target_table} VALUES ('{id}', '{name}', '{type}', '{dimention}', {resident_cnt});".format(
                target_table=TARGET_TABLE,
                id=location[0],
                name=location[1],
                type=location[2],
                dimention=location[3],
                resident_cnt=location[4]
            )
        pg_hook.run(sql)

    upload_ram_locations_to_gp = PythonOperator(
        task_id='upload_ram_locations_to_gp',
        python_callable=upload_ram_locations_to_gp_func,
        provide_context=True
    )

    dummy >> get_top3_ram_locations >> truncate_ram_table >> upload_ram_locations_to_gp