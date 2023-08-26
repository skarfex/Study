"""
Создаем таблицу в гринпламе p_valitov_13_ram_location с полями id, name, type, dimension, resident_cnt.
С помощью API находим три локации сериала "Рик и Морти"
"""

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from p_valitov_13_plugins.rick_and_morty_plugin import SearchTopLocation

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'p-valitov-13',
    'poke_interval': 600
}

with DAG("pvalitov_5_hw",
         schedule_interval=None,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['p-valitov-13']
         ) as dag:

    start_process = DummyOperator(task_id='start_process')

    create_or_truncate_table = PostgresOperator(
        task_id='create_or_truncate_table',
        postgres_conn_id='conn_greenplum',
        sql=[
            '''
            CREATE TABLE IF NOT EXISTS "public.p_valitov_13_ram_location"
            (
                id           INTEGER PRIMARY KEY,
                name         VARCHAR(256),
                type         VARCHAR(256),
                dimension    VARCHAR(256),
                resident_cnt INTEGER
            )
                DISTRIBUTED BY (id);''',
            '''TRUNCATE TABLE "public.p_valitov_13_ram_location";'''
        ],
        autocommit=True
    )

    search_top_location = SearchTopLocation(task_id='search_top_location')


    #DAG
    start_process >> create_or_truncate_table >> search_top_location