"""
Кладем в ГП три локации из Рика и Морти с наибольшим количеством резидентов
"""

from airflow import DAG
from datetime import datetime, timedelta
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from a_polina_plugins.a_polina_get_rick_and_morty_locations import APolinaWriteLocationsOperator

DEFAULT_ARGS = {
    'schedule_interval': '@once',
    'start_date': datetime(2023, 2, 14),
    'owner': 'a-polina',
    'poke_interval': 600
}

with DAG("a-polina-rick-and-morty",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-polina']
         ) as dag:
    table_name = 'a_polina_ram_location'


    def create_table():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        sql_statement = f'CREATE TABLE IF NOT EXISTS {table_name}\n(' \
                        f'id integer,\n' \
                        f'name varchar,\n' \
                        f'type varchar,\n' \
                        f'dimension varchar,\n' \
                        f'resident_cnt int);'
        with pg_hook.get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(sql_statement)
        conn.close()


    start = DummyOperator(task_id='start', dag=dag)

    create_table = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
        dag=dag
    )

    get_locations_from_API = APolinaWriteLocationsOperator(
        task_id='get_locations_from_API',
        dag=dag
    )

    start >> create_table >> get_locations_from_API
