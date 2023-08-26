"""
5 УРОК
РАЗРАБОТКА СВОИХ ПЛАГИНОВ
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from y_niyazbayev_plugins.y_niyazbyaev_ram_operators import YerzhanRamTop3LocationOperator

import logging


url = "https://rickandmortyapi.com/api/location"

DEFAULT_ARGS = {
    'owner': 'y_niyazbayev',
    'depends_on_past': False,
    'start_date': days_ago(1),
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
                   "type character varying(50), "
                   "dimension character varying(50), "
                   "resident_cnt integer);"
                   .format('y_niyazbayev_ram_location'))
    logging.info('Truncating table')
    cursor.execute(f'TRUNCATE TABLE y_niyazbayev_ram_location;')
    logging.info('Inserting rows')
    for location in locations_list:
        cursor.execute(
            "INSERT INTO y_niyazbayev_ram_location VALUES(%s, %s, %s, %s, %s);",
            (location[0], location[1], location[2], location[3], location[4])
        )
    conn.commit()
    logging.info('db updated')


with DAG("y_niyazbayev_homework_5",
         schedule_interval='@daily',
         catchup=True,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['y_niyazbayev']
         ) as dag:

    get_top3_locations = YerzhanRamTop3LocationOperator(
        task_id='get_top3_locations',
        url=url
    )

    load_locations_to_db = PythonOperator(
        task_id='load_locations_to_db',
        python_callable=load_to_db
    )

get_top3_locations >> load_locations_to_db
