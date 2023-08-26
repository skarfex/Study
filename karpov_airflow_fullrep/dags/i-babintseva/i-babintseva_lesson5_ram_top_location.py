"""
  This dag get top-3 locations for max count of residents and save info in GP table
"""

import logging
from airflow import DAG
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from i_babintseva_plugins.i_babintseva_ram_top_location_operator import RamTopLocationOperator


table = 'i_babintseva_ram_location'

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'i-babintseva',
    'poke_interval': 600
}

with DAG("i-babintseva_lesson5_ram_top_location",
         schedule_interval='@once',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['i-babintseva']
) as dag:

    start = DummyOperator(task_id='start')
    finish = DummyOperator(task_id='finish')

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_success',
        sql=f'''
            CREATE TABLE IF NOT EXISTS {table} (
                id varchar NOT NULL,
                name varchar NULL,
                type varchar NULL,
                dimension varchar NULL,
                resident_cnt int4 NULL,
                CONSTRAINT i_babintseva_ram_location_pkey PRIMARY KEY (id)
            )
            DISTRIBUTED BY (id);            
            ''',
        autocommit=True
    )

    get_top_locations = RamTopLocationOperator(
        task_id='get_top_locations',
        top_cnt=3
    )

    def reload_info_func(**kwargs):
        top_locations = kwargs['ti'].xcom_pull(task_ids='get_top_locations')
        if len(top_locations) > 0:
            pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
            pg_hook.run(f"TRUNCATE TABLE {table}")
            pg_hook.insert_rows(table=table, rows=top_locations, commit_every=1)
        else:
            logging.info('API get empty result')

    reload_info = PythonOperator(
        task_id='reload_info',
        python_callable=reload_info_func,
        dag=dag
    )

start >> create_table >> get_top_locations >> reload_info >> finish