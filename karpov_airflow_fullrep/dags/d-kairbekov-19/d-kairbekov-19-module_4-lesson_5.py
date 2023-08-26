"""
Module 4 Lesson 5
"""

# Logging
import logging

# DateTime
import datetime as dt

# Itemgetter
from operator import itemgetter

# AirFlow
from airflow import DAG
from airflow.utils.dates import days_ago

# Hooks
from airflow.hooks.postgres_hook import PostgresHook

# Operators
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Plugins
from d_kairbekov_19_ram_plugins.d_kairbekov_19_ram_top_location_operator import DKairbekov19RAMOperator


DEFAULT_ARGS = {
    "start_date": days_ago(1),
    "owner": "d-kairbekov-19",
    "poke_interval": 300,
}

with DAG(
    "d-kairbekov-19-module_4-lesson_5",
    schedule_interval="@once",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["d-kairbekov-19"],
) as dag:

    # Funcs #

    def get_top_locations(ti):
        locations = ti.xcom_pull(task_ids='locations', key='return_value')
        desc_sort_locations = sorted(locations, key=itemgetter('name'), reverse=True)
        ti.xcom_push(key="top_locations", value=desc_sort_locations[:2])

    # Tasks #

    dummy = DummyOperator(task_id="dummy")

    create_ram_location_table = PostgresOperator(
        task_id="create_ram_location_table",
        postgres_conn_id="conn_greenplum_write",
        trigger_rule='all_success',
        sql="""
            CREATE TABLE IF NOT EXISTS public.d_kairbekov_19_ram_location
            (
                id SMALLINT NOT NULL,
                name VARCHAR(256) NOT NULL,
                type VARCHAR(256) NOT NULL,
                dimension VARCHAR(256) NOT NULL,
                resident_cnt SMALLINT NOT NULL
            )
            DISTRIBUTED BY (dimension);

            TRUNCATE TABLE public.d_kairbekov_19_ram_location;
        """,
    )

    locations = DKairbekov19RAMOperator(
        task_id="locations",
    )

    top_locations = PythonOperator(
        task_id="top_locations",
        python_callable=get_top_locations,
        dag=dag,
    )

    insert_top_locations = PostgresOperator(
        task_id='insert_top_locations',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_success',
        sql='''
        INSERT INTO public.d_kairbekov_19_ram_location VALUES
        {{ ti.xcom_pull(task_ids='top_locations', key='top_locations') }}
        '''
    )

    # DAG #

    dummy >> locations >> create_ram_location_table >> top_locations >> insert_top_locations
