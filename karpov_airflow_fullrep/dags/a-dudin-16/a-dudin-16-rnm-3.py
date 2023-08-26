from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from a_dudin_16_plugins.a_dudin_16_ram_operator import DaaRNMOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a-dudin-16',
    'retries': 3,
    'retry_delay': timedelta(seconds=60),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=2)
}

with DAG("a-dudin-16_rnm_3",
          schedule_interval=None,
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-dudin-16']
          ) as dag:

    start = DummyOperator(
        task_id='start'
    )

    end = DummyOperator(
        task_id='end'
    )

    prepare_target_table = PostgresOperator(
        task_id='prepare_target_table',
        postgres_conn_id='conn_greenplum_write',
        sql="""
            CREATE TABLE IF NOT EXISTS a_dudin_16_ram_location
            (
                id integer PRIMARY KEY,
                name varchar(1024),
                type varchar(1024),
                dimension varchar(1024),
                resident_cnt integer
            )
            DISTRIBUTED BY (id);
        """,
        autocommit=True
    )

    load_rnm_top_locations = DaaRNMOperator(
        task_id='load_rnm_top_locations',
        top_location_count=3
    )

    start >> prepare_target_table >> load_rnm_top_locations >> end