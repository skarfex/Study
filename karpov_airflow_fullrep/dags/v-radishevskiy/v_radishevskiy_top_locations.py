"""
Тестовый dag, возвращающий заголовок articles в зависимости от дня недели.
Работает с понедельника по субботу

"""
from datetime import datetime
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from v_radishevskiy_plugins.v_radishevskiy_top_locations_operator import FetchTopLocations


DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'v-radishevskiy',
    'poke_interval': 600
}

with DAG(
        dag_id="v-radishevskiy-top-locations",
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['v-radishevskiy', 'test'],
        catchup=False,
) as dag:

    dummy_start = DummyOperator(task_id='start')

    create_table = PostgresOperator(
        task_id='create_truncate_table',
        postgres_conn_id='conn_greenplum_write',
        sql=[
        """
            CREATE TABLE IF NOT EXISTS "v-radishevskiy_ram_location"
            (
                "id" int4 NOT NULL,
                "name" varchar(256) NULL,
                "type" varchar(256) NULL,
                "dimension" varchar(256) NULL,
                "resident_cnt" int4 NULL
            );
        """,
        """TRUNCATE TABLE "v-radishevskiy_ram_location";"""
        ],
        autocommit=True
    )

    fetch_write_top_locations = FetchTopLocations(task_id='fetch_write_top_locations',)

    dummy_start >> create_table >> fetch_write_top_locations
