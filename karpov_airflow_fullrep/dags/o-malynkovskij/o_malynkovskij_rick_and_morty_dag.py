"""
Пайплайн по загрузке данных о трёх локациях сериала "Рик и Морти" с наибольшим количеством резидентов
из API ( https://rickandmortyapi.com/documentation/#location ) в таблицу GreenPlum
"""
import logging
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import BranchPythonOperator

from airflow.utils.dates import days_ago
from datetime import datetime, date

from o_malynkovskij_plugins.o_malynkovskij_top_loactions import OMalynkovskyTopLocationsOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'o-malynkovskij'
}

with DAG("o-malynkovskij_rick_and_morty_dag",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['o-malynkovskij']
) as dag:

    def is_empty_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        select_query = f'select * from o_malynkovskij_ram_location'
        cursor.execute(select_query)
        if cursor.rowcount == 0:
            result = 'dummy_task'
        else:
            result = 'delete_rows'
        return result


    dummy_task = DummyOperator(task_id="dummy_task")


    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        sql='''
            CREATE TABLE IF NOT EXISTS o_malynkovskij_ram_location (
                      id INTEGER PRIMARY KEY,
                      name VARCHAR,
                      type VARCHAR,
                      dimension VARCHAR,
                      residents_cnt INTEGER);
        ''',
        autocommit=True
    )

    is_empty = BranchPythonOperator(
        task_id='is_empty',
        python_callable=is_empty_func
    )

    delete_rows = PostgresOperator(
        task_id='delete_rows',
        postgres_conn_id='conn_greenplum_write',
        sql=f'TRUNCATE TABLE o_malynkovskij_ram_location',
        autocommit=True
    )

    get_locations = OMalynkovskyTopLocationsOperator(
        task_id='get_locations',
        trigger_rule='all_done'
    )

    insert_locations = PostgresOperator(
        task_id='insert_locations',
        postgres_conn_id='conn_greenplum_write',
        sql='''
            INSERT INTO o_malynkovskij_ram_location 
            VALUES {{ ti.xcom_pull(task_ids='get_locations', key='return_value') }}''',
        autocommit=True

    )

    create_table >> is_empty >> [dummy_task, delete_rows] >> get_locations >> insert_locations






