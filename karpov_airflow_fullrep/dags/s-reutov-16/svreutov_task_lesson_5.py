from airflow import DAG
from datetime import datetime
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from s_reutov_16.pluginOperatorRamLocations import SvrRamTopLocation

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'owner': 'svreutov',
    'poke_interval': 600
}

with DAG("s-reutov-16_lesson_5",
         schedule_interval=None,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['svreutov']
         ) as dag:

    start = DummyOperator(
        task_id='start'
    )

    create_table_gp = PostgresOperator(
        task_id='create_table_gp',
        postgres_conn_id='conn_greenplum_write',
        sql="""
                CREATE TABLE IF NOT EXISTS s_reutov_16_ram_location
                (
                    id integer PRIMARY KEY,
                    name varchar(1024),
                    type varchar(1024),
                    dimension varchar(1024),
                    resident_cnt integer
                )
                DISTRIBUTED BY (id);
            """
    )

    load_ram_top_locations = SvrRamTopLocation(
        task_id='load_ram_top_locations'
    )

    start >> create_table_gp >> load_ram_top_locations
