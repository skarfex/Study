"""
Топ-3 локации сериала "Рик и Морти" по количеству резидентов
"""

from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from a_dronov_20_plugins.a_dronov_ram_location import ADRONOV20RamTopLocationCountOperator

DEFAULT_ARGS = {
    'start_date': datetime(2023, 5, 1),
    'end_date': datetime(2023, 5, 4),
    'owner': 'a-dronov-20',
    'depends_on_past': True,
    'poke_interval': 600
}

with DAG("a-dronov-20_lab5",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-dronov-20']
         ) as dag:

    # Создание таблицы, если она не существует
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        sql='''
            CREATE TABLE IF NOT EXISTS a_dronov_20_ram_location (
                id int NOT NULL,
    	        name text NULL,
    	        type text NULL,
    	        dimension text NULL,
    	        resident_cnt int NOT NULL
            )
            DISTRIBUTED BY (id);
            ''',
        autocommit=True,
        dag=dag
    )

    # Очищаем таблицу для того, чтобы убрать результат предыдущего запуска
    truncate_table = PostgresOperator(
        task_id='truncate_table',
        postgres_conn_id='conn_greenplum_write',
        sql='TRUNCATE TABLE a_dronov_20_ram_location;',
        dag=dag
    )

    # Загрузка в таблицу location данных по топ-3 локаций по кол-ву резидентов
    top3_location = ADRONOV20RamTopLocationCountOperator(task_id='top3_location', dag=dag)

create_table >> truncate_table >> top3_location
