from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import logging
from d_gutyrchik_plugins.d_gutyrchik_locations_operator import LocationsGutyrchikOperator

DEFAULT_ARGS = {
    'owner': 'dgutyrchik',
    'start_date': days_ago(1)
}

dag = DAG(
    dag_id="d-gutyrchik-16-locations",
    schedule_interval=None,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['dgutyrchik-16']
)

create_dest_table = PostgresOperator(
    task_id="create_dest_table",
    postgres_conn_id='conn_greenplum_write',
    sql='''
        CREATE TABLE IF NOT EXISTS public.d_gutyrchik_16_ram_location (
                id int4 NOT NULL,
                "name" text NULL,
                "type" text NULL,
                dimension text NULL,
                resident_cnt int4 NULL,
                CONSTRAINT d_gutyrchik_16_ram_location_pk PRIMARY KEY (id)
            );
        TRUNCATE TABLE "d_gutyrchik_16_ram_location";
        ''',
    autocommit=True,
    dag=dag
)

insert_locations = LocationsGutyrchikOperator(task_id='insert_locations', dag=dag)

create_dest_table >> insert_locations
