"""
Вывод топ 3 локаций по количеству существ в Рике и Морти
"""

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


from d_kuzmin_11_plugins.rick_and_morty_api_operator import RickMortyOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'd-kuzmin-11',
    'poke_interval': 600
}

dag = DAG("d-kuzmin-11_locations",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['d-kuzmin-11']
          )


start = DummyOperator(task_id='start', dag=dag)


create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='conn_greenplum_write',
    sql='''
        CREATE TABLE IF NOT EXISTS d_kuzmin_11_ram_location (
            id            INTEGER PRIMARY KEY,
            name          VARCHAR NOT NULL,
            type          VARCHAR NOT NULL,
            dimension     VARCHAR NOT NULL,
            resident_cnt  INTEGER
        );
        TRUNCATE TABLE "d_kuzmin_11_ram_location";
        ''',
    autocommit=True,
    dag=dag
)

insert_top_locations = RickMortyOperator(task_id='find_locations', dag=dag)


start >> create_table >> insert_top_locations

create_table.doc_md = """Удаляем все значения из таблицы если она существует, если нет - создаем ее"""
insert_top_locations.doc_md = """Находим локации с наибольшим числом существ и добавляем их в таблицу"""
