"""
HW. Lesson 5. Get top 3 location from Rick&Morty API
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import logging
import pendulum


from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

from a_sokolov_6_plugins.a_sokolov_6_rick_and_morty_operator import ASokolovGetTopLocationOperator

DEFAULT_ARGS = {
    'owner': 'a-sokolov-6',
    'start_date': days_ago(3),
    'poke_interval': 300
}

with DAG("a-sokolov-6-lesson-5",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=2,
         tags=['ais', 'lesson5']
) as dag:

    start = DummyOperator(task_id="start")

    end = DummyOperator(task_id="end")

    create_table = PostgresOperator(
    task_id="create_table",
    postgres_conn_id='conn_greenplum_write',
    sql='''
            create table if not exists PUBLIC.A_SOKOLOV_6_RAM_LOCATION (
            id            integer,
            name          text,
            type          text,
            dimension     text,
            resident_cnt  integer
            );
           /* truncate PUBLIC.A_SOKOLOV_6_RAM_LOCATION; */
        ''',
    autocommit=True,
    dag=dag
    )

    get_top_locations = ASokolovGetTopLocationOperator(task_id='get_top_locations', dag=dag)


    start >> create_table >> get_top_locations >> end