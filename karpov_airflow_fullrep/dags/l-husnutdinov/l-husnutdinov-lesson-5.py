# -*- coding: utf-8 -*-
"""
Rick and Morty DAG
"""

from datetime import datetime
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

from l_husnutdinov_plugins.ram_operator import RickAndMortyTopLocationsByResidents


DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'l-husnutdinov'
}

with DAG(
        dag_id='l-husnutdinov-lesson-5',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['lesson-5', 'l-husnutdinov']
) as dag:
    @task(task_id="createTable")
    def create_table():
        logging.info("Create table l-husnutdinov_ram_location if not exist")

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

        with pg_hook.get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS public.l_husnutdinov_ram_location (
                                id INT4 PRIMARY KEY,
                                name VARCHAR NOT NULL,
                                type VARCHAR NOT NULL,
                                dimension VARCHAR NOT NULL,
                                resident_cnt INT4 NOT NULL);
                                ''')

    createTableOp = create_table()

    apiRequestOp = RickAndMortyTopLocationsByResidents(task_id="apiRequest",
                                                       top_n=3)

    @task(task_id="insertIntoTable")
    def insert_into_table(**kwargs):
        logging.info("Insert into table")

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

        with pg_hook.get_conn() as conn:
            cursor = conn.cursor()

            top_locations = kwargs['ti'].xcom_pull(task_ids='apiRequest')

            logging.info(top_locations)

            for l in top_locations:
                cursor.execute(f"INSERT INTO public.l_husnutdinov_ram_location VALUES ( \
                                 {l['id']}, '{l['name']}', '{l['type']}', '{l['dimension']}', {l['residents']})")

    insertIntoTableOp = insert_into_table()

    createTableOp >> apiRequestOp >> insertIntoTableOp
