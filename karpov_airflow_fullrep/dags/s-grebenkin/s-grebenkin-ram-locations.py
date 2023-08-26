'''
Урок 5: Загружаем три локации сериала "Рик и Морти"
с наибольшим количеством резидентов в таблицу GreenPlum
'''
import logging
from datetime import datetime

from airflow import DAG

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from s_grebenkin_plugins.s_grebenkin_ram_locations_operator import GrebenkinRamLocationsOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 6, 26),
    'owner': 's-grebenkin',
    'retries': 3,
    'poke_interval': 20
}

with DAG('s-grebenkin-ram-locations',
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         catchup=True,
         tags=['s-grebenkin']
         ) as dag:

    start = DummyOperator(task_id='start')

    file_path = '/tmp/'
    file_name = 'Grebenkin_ram.csv'
    table_name = 's_grebenkin_ram_location'

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

    find_top_three_locations = GrebenkinRamLocationsOperator(
        task_id='find_top_three_locations'
    )

    def load_csv_to_gp_func():
        pg_hook = PostgresHook('conn_greenplum_write')
        pg_hook.copy_expert(f"COPY {table_name} FROM STDIN DELIMITER ','", f'{file_path}{file_name}')


    load_csv_to_gp = PythonOperator(
        task_id='load_csv_to_gp',
        python_callable=load_csv_to_gp_func
    )

    end = DummyOperator(task_id='end')

    start >> remove_old_files >> create_and_empty_table >> find_top_three_locations >> load_csv_to_gp >> end
