from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from a_evteev_plugins.ram_topN_location_operator import RamTopNLocationOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a-evteev',
    'retries': 5,
    'retry_delay': timedelta(seconds=30),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=1),
}

with DAG("a-evteev-top-ram-location",
          schedule_interval=None,
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-evteev']
          ) as dag:

    start = DummyOperator(
        task_id='start'
    )

    end = DummyOperator(
        task_id='end'
    )

    """
    Создание таблицы a_evteev_ram_location, если ее не существует
    """
    prepare_target_table = PostgresOperator(
        task_id='prepare_target_table',
        postgres_conn_id='conn_greenplum_write',
        sql="""
            CREATE TABLE IF NOT EXISTS a_evteev_ram_location
            (
                id integer PRIMARY KEY,
                name varchar(1024),
                type varchar(1024),
                dimension varchar(1024),
                resident_cnt integer
            )
            DISTRIBUTED BY (id);
        """,
        autocommit=True,
    )

    """
    Загрузка данных по топ локациям из API по Рик и Морти в таблицу a_evteev_ram_location
    количество локаций, загружаемых в таблицу, можно указать в параметре оператора top_location_cnt
    """
    load_ram_top_locations = RamTopNLocationOperator(
        task_id='load_ram_top_locations',
        top_location_cnt=3
    )

    start >> prepare_target_table >> load_ram_top_locations >> end

