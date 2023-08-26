import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

from n_ponomarev.n_ponomarev_ram_operator import NPonomarevRamOperator

with DAG("ponomarev_ram_dag",
         start_date=datetime.now() - timedelta(days=1),
         schedule_interval = '@daily',
         tags=['np', 'n_ponomarev']
) as dag:

    start_pipe = DummyOperator(
        task_id = 'start_pipe'
        )

    creating_table = PostgresOperator(
        task_id='creating_table',
        postgres_conn_id='conn_greenplum_write',
        database='students',
        sql="""
        create table if not exists n_ponomarev_ram_location(
        id text,
        name text,
        type text,
        dimension text,
        resident_cnt text
        )
        """
    )

    top_locations_get_and_push = NPonomarevRamOperator(
        task_id='top_locations_get_and_push',
        top=3
    )

    clean_data = PostgresOperator(
        task_id='clean_data',
        postgres_conn_id = 'conn_greenplum_write',
        database='students',
        sql=["""
        TRUNCATE TABLE n_ponomarev_ram_location
        """]
    )

    def load_csv_to_gp_func():
        pg_hook = PostgresHook('conn_greenplum_write')
        pg_hook.copy_expert("COPY n_ponomarev_ram_location FROM STDIN DELIMITER ','", '/tmp/n_ponomarev_ram.csv')


    load_csv_to_gp = PythonOperator(
        task_id='load_csv_to_gp',
        python_callable=load_csv_to_gp_func
    )

    start_pipe >> creating_table >> top_locations_get_and_push >> clean_data >> load_csv_to_gp

