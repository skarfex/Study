"""
Задание к уроку №5 "Разработка своих плагинов" v2
"""
from airflow import DAG
from datetime import datetime
import logging

from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from v_zotov_plugins.v_zotov_ram_operators import ZotovRamTop3LocationOperator

url = "https://rickandmortyapi.com/api/location"

DEFAULT_ARGS = {
    'start_date': datetime(2022, 6, 1),
    'end_date': datetime(2022, 6, 20),
    'owner': 'v-zotov',
    'poke_interval': 600
}


def load_to_db(**kwargs):
    ti = kwargs['ti']
    locations_list = ti.xcom_pull(task_ids='get_top3_locations')
    logging.info(f'locations_list: {locations_list}')

    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    logging.info('Creating table')
    cursor.execute("CREATE TABLE IF NOT EXISTS {}"
                   "(id integer, "
                   "name character varying(50), "
                   "type character varying(35), "
                   "dimension character varying(50), "
                   "resident_cnt integer);"
                   .format('v_zotov_ram_location'))
    logging.info('Truncating table')
    cursor.execute(f'TRUNCATE TABLE v_zotov_ram_location;')
    logging.info('Inserting rows')
    for location in locations_list:
        cursor.execute(
            "INSERT INTO v_zotov_ram_location VALUES(%s, %s, %s, %s, %s);",
            (location[0], location[1], location[2], location[3], location[4])
        )
    conn.commit()
    logging.info('db updated')


with DAG("v_zotov_lesson5_v3",
         schedule_interval='@daily',
         catchup=True,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['v-zotov']
         ) as dag:

    get_top3_locations = ZotovRamTop3LocationOperator(
        task_id='get_top3_locations',
        url=url
    )

    load_locations_to_db = PythonOperator(
        task_id='load_locations_to_db',
        python_callable=load_to_db
    )
    get_top3_locations >> load_locations_to_db
