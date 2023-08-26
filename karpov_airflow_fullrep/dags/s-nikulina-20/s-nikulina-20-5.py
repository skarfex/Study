from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from s_nikulina_20_plugins.s_nikulina_20_ram_locations import SNikulinaRamMortyLocationsOperator

import logging

url = "https://rickandmortyapi.com/api/location"

DEFAULT_ARGS = {
    'owner': 's-nikulina-20',
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
    cursor.execute("CREATE TABLE IF NOT EXISTS s_nikulina_20_ram_location "
                   "(id integer, "
                   "name character varying(255), "
                   "type character varying(255), "
                   "dimension character varying(255), "
                   "resident_cnt integer);")
    logging.info('Truncating table')
    cursor.execute("TRUNCATE TABLE s_nikulina_20_ram_location;")
    logging.info('Inserting rows')
    for location in locations_list:
        cursor.execute(
            "INSERT INTO s_nikulina_20_ram_location VALUES(%s, %s, %s, %s, %s);",
            (location[0], location[1], location[2], location[3], location[4])
        )
    conn.commit()
    logging.info('db updated')
    logging.info('Inserted rows count: %s', cursor.rowcount)


with DAG("s-nikulina-20_lesson_5",
         schedule_interval='@daily',
         catchup=True,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['s_nikulina-20']
         ) as dag:

    get_top3_locations = SNikulinaRamMortyLocationsOperator(
        task_id='get_top3_locations',
        url=url
    )

    load_locations_to_db = PythonOperator(
        task_id='load_locations_to_db',
        python_callable=load_to_db
    )

get_top3_locations >> load_locations_to_db