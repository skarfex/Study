from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

from s_abdibekov_plugins.rick_and_morty import TopLocationsRickAndMorty

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 's-abdibekov-17',
    'poke_interval': 600
}

with DAG("s-abdibekov-17_rick_and_morty2",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['s-abdibekov-17', 'rick_and_morty']
         ) as dag:

    start_process = DummyOperator(task_id='start_process')

    create_or_truncate_table = PostgresOperator(
        task_id='create_or_truncate_table',
        postgres_conn_id='conn_greenplum_write',
        sql=[
            '''
            CREATE TABLE IF NOT EXISTS "s_abdibekov_17_ram_location"
            (
                id           INTEGER PRIMARY KEY,
                name         VARCHAR(256),
                type         VARCHAR(256),
                dimension    VARCHAR(256),
                resident_cnt INTEGER
            )
                DISTRIBUTED BY (id);
                
                
                ''',
            '''TRUNCATE TABLE s_abdibekov_17_ram_location;'''
        ],
        autocommit=True
    )

    top_locations_rick_and_morty = TopLocationsRickAndMorty(task_id='top_locations_rick_and_morty')

    end_process = DummyOperator(task_id='end_process')

    start_process >> create_or_truncate_table >> top_locations_rick_and_morty >> end_process
