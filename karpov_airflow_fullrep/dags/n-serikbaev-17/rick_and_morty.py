"""
n-serikbaev-17-lesson-5
rick and morty top locations by resident count
"""

from airflow import DAG

from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from n_serikbaev_17_plugins.n_serikbaev_17_ram_location import NSerikbaev17RaMLocationOperator

import psycopg2

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'n-serikbaev-17',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(seconds=5),
}

with DAG("n-serikbaev-17-lesson-5",
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['n-serikbaev-17']
          ) as dag:

    get_top_locations = NSerikbaev17RaMLocationOperator(
        task_id='get_top_locations',
        special_number=3,
        dag=dag)


    def if_table_exists():
        get_sql = f"select * from n_serikbaev_17_ram_location;"
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(get_sql)
            return 'clear_table'
        except psycopg2.errors.UndefinedTable:
            print("No such table. Create new table")
            locked = True
            return 'all_ok'

    exists_branch = BranchPythonOperator(
        task_id='exists_branch',
        python_callable=if_table_exists,
        dag=dag)

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='one_success',
        sql='''
        create table if not exists 
        n_serikbaev_17_ram_location (
                    id SERIAL4 PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    type VARCHAR NOT NULL,
                    dimension VARCHAR NOT NULL,
                    residents_cnt INT4 NOT NULL);
        ''',
        dag=dag)

    all_ok = DummyOperator(task_id='all_ok', dag=dag)

    clear_table = PostgresOperator(
        task_id='clear_table',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_done',
        sql='truncate table n_serikbaev_17_ram_location;',
        dag=dag)

    write_locations = PostgresOperator(
        task_id='write_locations',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_done',
        sql='''
            insert into n_serikbaev_17_ram_location values 
            {{ ti.xcom_pull(task_ids='get_top_locations', key='return_value') }};
            ''',
        dag=dag)

    get_top_locations >> exists_branch >> [all_ok, clear_table]
    all_ok >> create_table >> write_locations
    clear_table >> write_locations