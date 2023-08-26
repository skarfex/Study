from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

from a_martyshkin_15_plugins.a_martyshkin_15_ram_top_locations import martyshkin_rick_and_morty

DEFAULT_ARGS = {
    'start_date': days_ago(0),
    'owner': 'a-martyshkin-15',
    'poke_interval': 600
}

with DAG("martyshkin_rick_and_morty",
         schedule_interval=None,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-martyshkin-15']
         ) as dag:

    create_or_truncate_table = PostgresOperator(
        task_id='create_or_truncate_table',
        postgres_conn_id='conn_greenplum_write',
        sql=[
            '''
            CREATE TABLE IF NOT EXISTS "a-martyshkin-15_ram_location"
            (
                id           text,
                name         text,
                type         text,
                dimension    text,
                resident_cnt text
            );''',
            '''TRUNCATE TABLE "a-martyshkin-15_ram_location";'''
        ],
        autocommit=True
    )

    top_locations_rick_and_morty = martyshkin_rick_and_morty(task_id='top_locations_rick_and_morty')



    create_or_truncate_table >> top_locations_rick_and_morty