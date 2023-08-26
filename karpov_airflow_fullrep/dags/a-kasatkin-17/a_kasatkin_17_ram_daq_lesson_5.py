import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from a_kasatkin_17_plugins.a_kasatkin_17_plugin import Kasatkin_ram_toploc

with DAG("a_kasatkin_17_ram_daq_lesson_5",
         start_date = datetime.now() - timedelta(days=1),
         schedule_interval = '@daily',
         tags = ['a_kasatkin_17']
) as dag:

    start = DummyOperator(
        task_id = 'start'
        )

    top_locations_get = Kasatkin_ram_toploc(
        task_id = 'toploc_get',
        tops=3
    )

    top_locations_load_to_bd = PostgresOperator(
        task_id = 'toploc_load_to_bd',
                postgres_conn_id = 'conn_greenplum_write',
        database = 'students',
        sql = ["""
        TRUNCATE TABLE a_kasatkin_17_ram_location
        """,
        """
        insert into a_kasatkin_17_ram_location values {{ti.xcom_pull(task_ids='toploc_get')}}
        """]
    )

    start  >> top_locations_get >> top_locations_load_to_bd


