"""
DAG for getting top 3 Rick and Morty's locations by count of residents and
writing them to Grenplum DB table 
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.xcom import XCom

from datetime import datetime

from ra_valiev_plugins.ra_valiev_ram_operator import Ra_Valiev_Search_Loc_Operator

TABLE_NAME = 'ra_valiev_ram_location'

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'ra-valiev',
    'poke_interval': 600
}

with DAG('ra-valiev_ram_location',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['ra-valiev']
) as dag:
    
    start_task = DummyOperator(task_id='Start_DAG')

    get_top_three_loc = Ra_Valiev_Search_Loc_Operator(
        task_id = 'get_top_three_loc'
    )
    
    create_table_task = PostgresOperator(
        task_id='create_table_task',
        postgres_conn_id='conn_greenplum_write',
        database='students',
        sql="CREATE TABLE IF NOT EXISTS ra_valiev_ram_location(id int not null, name varchar, type varchar, dimension varchar, resident_cnt int)"
    )
    
    def put_data_into_table_func(**kwargs):
        get_top_three = kwargs['ti'].xcom_pull(task_ids='get_top_three_loc', key='top_three_list')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run(f'TRUNCATE TABLE {TABLE_NAME}')
        logging.info(f'Таблица {TABLE_NAME} очищена')
        for loc in get_top_three:
            pg_hook.run(f"INSERT INTO {TABLE_NAME} VALUES ({loc['id']}, '{loc['name']}', '{loc['type']}', '{loc['dimension']}',{loc['residents_count']})")
        logging.info(f'Данные ТОП 3 локаций Rick and Morty API записаны в таблицу {TABLE_NAME}')

    put_data_into_table = PythonOperator(
        task_id='put_data_into_table',
        python_callable=put_data_into_table_func
    )

    end_task = DummyOperator(
        task_id='End_of_DAG',
        trigger_rule='one_success'
    )

start_task >> get_top_three_loc >> create_table_task >> put_data_into_table >> end_task