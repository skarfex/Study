"""
ДАГ, выгружающий в Greenplum
топ 3 локаций по количеству резидентов
из Rick and Morty с помощью API
"""

import logging

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from e_tsapko_plugins.e_tsapko_ram_location import ETsapkoRamLocationOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'e-tsapko',
    'poke_interval': 120
}

dag = DAG("e-tsapko_rick_and_morty",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['e-tsapko']
          )

# Удаляет таблицу в Greenplum, если она существует
drop_sql_query = """
DROP TABLE IF EXISTS public.e_tsapko_ram_location;
"""

drop_table = PostgresOperator(
    task_id="drop_table",
    sql=drop_sql_query,
    postgres_conn_id='conn_greenplum',
    dag=dag
)

# Создает таблицу в Greenplum
create_table_query = """
CREATE TABLE IF NOT EXISTS public.e_tsapko_ram_location (
    id TEXT,
    name TEXT,
    type TEXT,
    dimension TEXT,
    resident_cnt TEXT
);
"""

create_table = PostgresOperator(
    task_id="create_table",
    sql=create_table_query,
    postgres_conn_id='conn_greenplum',
    dag=dag
)

# Загружает топ 3 локаций по количеству персонажей в Greenplum
load_top3_locations = ETsapkoRamLocationOperator(
    task_id="load_top3_locations",
    top_n=3,
    ascending=False,
    dag=dag
)

drop_table >> create_table >> load_top3_locations
