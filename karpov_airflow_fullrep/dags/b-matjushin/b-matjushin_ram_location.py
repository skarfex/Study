"""
С помощью API (https://rickandmortyapi.com/documentation/#location) найдите
три локации сериала "Рик и Морти" с наибольшим количеством резидентов.
"""

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


from b_matjushin_plugins.b_matjushin_rickandmortyapi_operator import RickAndMortyApiOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'b-matjushin',
    'poke_interval': 600
}

with DAG("b-matjushin_ram_locations",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['b-matjushin']
          ) as dag:


    start = DummyOperator(task_id='start')

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        sql='''
            DROP TABLE IF EXISTS b_matjushin_ram_location;
        
            CREATE TABLE IF NOT EXISTS b_matjushin_ram_location (
                id            INTEGER PRIMARY KEY,
                name          VARCHAR NOT NULL,
                type          VARCHAR NOT NULL,
                dimension     VARCHAR NOT NULL,
                resident_cnt  INTEGER
            );
            
            SELECT setval('id', (SELECT max(id) FROM b_matjushin_ram_location)+1);
            ''',
        autocommit=True
    )

    find_locations = RickAndMortyApiOperator(task_id='find_locations').execute()


start >> create_table >> find_locations

create_table.doc_md = """Удаляем все значения из таблицы если она существует, если нет - создаем ее"""
find_locations.doc_md = """Находим локации с наибольшим числом существ и добавляем их в таблицу"""
