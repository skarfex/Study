"""
Get top-3 locations from Rick and Morty API and insert to Greenplum
"""

from airflow import DAG
import json
from datetime import datetime

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from m_mashrapov_9_plugins.mashr_ram_operator import MashrRamOperator

DEFAULT_ARGS = {
    'owner': 'm-mashrapov-9',
    'poke_interval': 600,
    'start_date': datetime(2022, 6, 19),
    'end_date': datetime(2022, 6, 30)
}

with DAG(
        "m-mashrapov-9_ram_locations",
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['m-mashrapov-9']
) as dag:

    start = DummyOperator(task_id='start')

    get_top3_ram_locations_from_API = MashrRamOperator(
        task_id='get_top3_ram_locations_from_API'
    )

    def insert_top3_ram_locations_to_gp_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        gp_table = 'm_mashrapov_9_ram_locations'
        sql_truncate = f'truncate table {gp_table}'
        sql_insert = ''
        pg_hook.run(sql_truncate)
        ram_locations = json.loads(kwargs['ti'].xcom_pull(task_ids='get_top3_ram_locations_from_API', key='return_value'))
        for location in ram_locations:
            sql_insert += "insert into {table} (id, name, type, dimension, resident_cnt) values ({id}, '{name}', '{type}', '{dimension}', {resident_cnt});".format(
                table=gp_table,
                id=location['id'],
                name=location['name'],
                type=location['type'],
                dimension=location['dimension'],
                resident_cnt=location['resident_cnt']
            )
        pg_hook.run(sql_insert)
        conn.close

    insert_top3_ram_locations_to_gp = PythonOperator(
        task_id='insert_top3_ram_locations_to_gp',
        python_callable=insert_top3_ram_locations_to_gp_func,
        provide_context=True
    )

    eod = DummyOperator(task_id='eod')

start >> get_top3_ram_locations_from_API >> insert_top3_ram_locations_to_gp >> eod