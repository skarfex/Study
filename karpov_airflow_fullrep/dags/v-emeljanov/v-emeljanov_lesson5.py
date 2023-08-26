"""
Lesson 5 - plugins:
This DAG
- prepare table in Greenplum database
- extracts top 3 locations with max residents count using API (https://rickandmortyapi.com/documentation/#location)
- loads top locations to Greenplum database

* The DAG uses both PostgresHook and PostgresOperator for training purpose
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from v_emeljanov_plugins.v_emeljanov_ram_top3locations_operator import VEmeljanovRamTopLocationsOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v_emeljanov',
    'poke_interval': 600
}

with DAG("v_emeljanov_lesson5",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['v-emeljanov']
         ) as dag:

    def ram_load_top_locations_py(**kwargs):
        top_rows = kwargs['ti'].xcom_pull(task_ids='ram_extract_top_locations')    # pull top rows from xcom
        top_data = ','.join(map(str, map(tuple, top_rows)))     # list to str for insert

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        sql = f"""
        INSERT INTO public."v-emeljanov_ram_location" (
            id,
            name,
            "type",
            dimension,
            resident_cnt
        ) 
        VALUES {top_data}
        """
        pg_hook.run(sql)

        logging.info(f'Loaded top rows: {top_data}')

    ram_create_or_truncate_table = PostgresOperator(
        task_id="ram_create_or_truncate_table",
        postgres_conn_id="conn_greenplum_write",
        sql="""
            CREATE TABLE IF NOT EXISTS public."v-emeljanov_ram_location" (
                id int NOT NULL,
                name text NOT NULL,
                "type" text,
                dimension text,
                resident_cnt int
            )
            DISTRIBUTED RANDOMLY;
            
            TRUNCATE TABLE public."v-emeljanov_ram_location";
            """,
    )   # postgres operator to create or truncate table

    ram_extract_top_locations = VEmeljanovRamTopLocationsOperator(
        task_id='ram_extract_top_locations',
        top_cnt=3
    )   # top locations operator to extract data using API

    ram_load_top_locations = PythonOperator(
        task_id='ram_load_top_locations',
        python_callable=ram_load_top_locations_py,
        provide_context=True,
        dag=dag
    )   # python operator to load top locations data into Greenplum database

    ram_create_or_truncate_table >> ram_extract_top_locations >> ram_load_top_locations
