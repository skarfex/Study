from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from v_glushak_plugins.glushak_ram_location_operator import GlushakRamLocationOperator

DEFAULT_ARGS = {
    'owner': 'glushak',
    'start_date': days_ago(2),
    'poke_interval': 600,
    'email': ['v.yu.glushak@tmn2.etagi.com'],
    'email_on_failure': True
}


with DAG("glushak_load_ram_gp",
          schedule_interval='@once',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['glushak'],
          catchup=True,
) as dag:

    create_table_ram = PostgresOperator(
        task_id='create_table_ram_location',
        postgres_conn_id='conn_greenplum_write',
        sql=f"""
                CREATE TABLE IF NOT EXISTS public.v_glushak_ram_location (
                id integer PRIMARY KEY,
                name text,
                type text,
                dimension text,
                resident_cnt integer);
                 """,
        autocommit=True,
    )

    insert_resident_cnt_locations = GlushakRamLocationOperator(
        task_id='load_gp_most_resident_cnt_locations'
    )


    def print_resident_cnt_locations():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(f'SELECT * FROM v_glushak_ram_location')
        query_res = cursor.fetchall()

        logging.info('--------------')
        logging.info(f'query_res: {query_res}')
        logging.info('--------------')


    print_locations = PythonOperator(
        task_id='print_resident_cnt_locations',
        python_callable=print_resident_cnt_locations
    )

    create_table_ram >> insert_resident_cnt_locations >> print_locations
