"""
This DAG uses self-written Airflow Operator
that calculates top three R&M locations by resident count;
the DAG creates table in Greenplum
and writes top three locations info into it.
"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow import DAG
import logging
from datetime import datetime, timedelta
from v_kosse_plugins.v_kosse_ram_location_operator import KosseTopRamLocationsOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 6, 10),
    'owner': 'v-kosse',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
        "kosse-rick-and-morty",
        schedule_interval='0 10 * * *',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        catchup=True,
        tags=['v-kosse', 'lesson_5']
) as dag:
    file_path = '/tmp/'
    file_name = 'Kosse_ram.csv'
    table_name = 'v_kosse_ram_location'

    remove_old_files = BashOperator(
        task_id='remove_old_files',
        bash_command=f'rm -f {file_path}{file_name}'
    )


    def create_and_empty_table_func():
        pg_hook = PostgresHook('conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id int, 
                    name varchar, 
                    type varchar, 
                    dimension varchar, 
                    resident_cnt int);
                TRUNCATE TABLE {table_name};
                """
        cursor.execute(sql)
        logging.info(f'table {table_name} created and empty')
        conn.commit()
        conn.close()


    create_and_empty_table = PythonOperator(
        task_id='create_and_empty_table',
        python_callable=create_and_empty_table_func
    )

    '''
    create_and_empty_table = PostgresOperator(
        task_id='create_and_empty_table',
        postgres_conn_id='conn_greenplum_write',
        sql=f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id int,
            name varchar,
            type varchar,
            dimension varchar,
            resident_cnt int);
        TRUNCATE TABLE {table_name};
        """
    )
    '''
    find_top_three_locations = KosseTopRamLocationsOperator(
        task_id='find_top_three_locations'
    )


    def load_csv_to_gp_func():
        pg_hook = PostgresHook('conn_greenplum_write')
        pg_hook.copy_expert(f"COPY {table_name} FROM STDIN DELIMITER ','", f'{file_path}{file_name}')


    load_csv_to_gp = PythonOperator(
        task_id='load_csv_to_gp',
        python_callable=load_csv_to_gp_func
    )

remove_old_files >> create_and_empty_table >> find_top_three_locations >> load_csv_to_gp
