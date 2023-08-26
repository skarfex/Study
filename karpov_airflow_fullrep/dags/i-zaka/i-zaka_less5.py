from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from i_zaka_plugins.i_zaka_rick_and_morty_operator import TopLocationsRickAndMorty

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'i-zaka',
    'poke_interval': 600
}

with DAG("i-zaka_rick_and_mortyv2",
         schedule_interval='@once',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['i-zaka', 'i-zaka_rick_and_morty']
         ) as dag:

    start_process = DummyOperator(task_id='start_process_')

    create_or_truncate_table = PostgresOperator(
        task_id='create_or_truncate_table',
        postgres_conn_id='conn_greenplum_write',
        sql=[
            '''
            CREATE TABLE IF NOT EXISTS "i_zaka_ram_location"
            (
                id           INTEGER PRIMARY KEY,
                name         VARCHAR(256),
                type         VARCHAR(256),
                dimension    VARCHAR(256),
                resident_cnt INTEGER
            )
                DISTRIBUTED BY (id);''',
            '''TRUNCATE TABLE "i_zaka_ram_location";'''
        ],
        autocommit=True
    )

    top_locations_rick_and_morty = TopLocationsRickAndMorty(task_id='top_locations_rick_and_morty')

    end_process = DummyOperator(task_id='end_process')

    start_process >> create_or_truncate_table >> top_locations_rick_and_morty >> end_process
