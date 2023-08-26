"""
Rick and Morty
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime


from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from v_burov_12_plugins.v_burov_12_location_operator import VBLocationsOperator


DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'v-burov-12',
}

with DAG("v-burov-12_dag_location",
          schedule_interval=None,
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['v-burov-12']
          ) as dag:

    dummy = DummyOperator(task_id="dummy")


    def create_table_for_api():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        sql_statement = f"""
            create table if not exists v_burov_12_ram_location
            (id integer,
            name text,
            type text,
            dimension text,
            resident_cnt integer)        
            """
        pg_hook.run(sql_statement, False)        
        logging.info("logging: table is created")

    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table_for_api
    )

    write_data_task = VBLocationsOperator(
        task_id='write_data',
        api_link="https://rickandmortyapi.com/api"
    )


dummy >> create_table_task >> write_data_task