import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from a_lazarev.r_a_m_lesson_5 import LazarevRaMLocationsTop

with DAG("a_lazarev_dag_r_a_m_lesson_5",
         start_date = datetime.now() - timedelta(days=1),
         schedule_interval = '@daily',
         tags = ['a-lazarev']
) as dag:

    start_pipe = DummyOperator(
        task_id = 'start_pipe'
        )

    creating_table = PostgresOperator(
        task_id = 'creating_table',
        postgres_conn_id = 'conn_greenplum_write',
        database = 'students',
        sql = """
        create table if not exists a_lazarev_top_ram(
        id int,
        name varchar,
        type varchar,
        dimension varchar,
        resident_cnt int
        )
        """
    )

    top_locations_get_and_push = LazarevRaMLocationsTop(
        task_id = 'top_locations_get_and_push',
        tops=3
    )

    top_locations_load_to_gp = PostgresOperator(
        task_id = 'top_locations_load_to_gp',
                postgres_conn_id = 'conn_greenplum_write',
        database = 'students',
        sql = ["""
        TRUNCATE TABLE a_lazarev_top_ram
        """,
        """
        insert into public.a_lazarev_top_ram values {{ti.xcom_pull(task_ids='top_locations_get_and_push')}}
        """]
    )

    start_pipe  >> creating_table >> top_locations_get_and_push >> top_locations_load_to_gp


