"""
5 урок 1 задание
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime
import pandas as pd

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator

from airflow.decorators import task
from airflow.decorators import dag

from m_repin_plugins.m_repin_rick_and_morty import RMLTopThreeLocationsOperator

DEFAULT_ARGS = {
    'start_date': '2023-02-18',
    'owner': 'm-repin',
    'poke_interval': 600
}

sql_exists = '''
    SELECT EXISTS (
        SELECT FROM 
            pg_tables
        WHERE 
            schemaname = 'public' AND 
            tablename  = 'm_repin_ram_location'
        );
'''
sql_drop = '''
    DROP TABLE IF EXISTS m_repin_ram_location 
'''

create_table_sql = '''
    CREATE TABLE IF NOT EXISTS m_repin_ram_location (
        id integer,
        name varchar(250),
        type varchar(100),
        dimension varchar(100),
        resident_cnt integer,
        primary key (id)
        )
    '''

with DAG("m_repin_rick_and_morty_dag",
    schedule_interval=None,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['m_repin']) as dag:
    
    
    start = DummyOperator(task_id='start')
    
    
    get_data_from_api = RMLTopThreeLocationsOperator(
        task_id = 'get_data_from_api'
    )
    
    
    def is_table_exists():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # и именованный (необязательно) курсор

        cursor.execute(sql_exists)  # исполняем sql
        try:
            query_res = cursor.fetchall()  # полный результат
            answer = query_res[0][0]
            logging.info(f"The query return {answer}")
            if answer:
                logging.info("Table does exists")
                cursor.execute(sql_drop)
                logging.info("Table has been dropped")
                logging.info("Table will be created")
                cursor.execute(create_table_sql)
                logging.info("Table has been successfully created")
                conn.commit()
                cursor.close()
                logging.info("Cursor has been closed")
                pg_hook.copy_expert("COPY m_repin_ram_location FROM STDIN DELIMITER ','", '/tmp/m_repin_ram_location.csv')
                logging.info("Table has been copyed")
            else:
                logging.info("Table will be created")
                cursor.execute(create_table_sql)
                logging.info("Table has been successfully created")
                conn.commit()
                cursor.close()
                logging.info("Cursor has been closed")
                pg_hook.copy_expert("COPY m_repin_ram_location FROM STDIN DELIMITER ','", '/tmp/m_repin_ram_location.csv')
                logging.info('The table has been successfully copyed')
        except:
            one_string = cursor.fetchone()[0]  # если вернулось единственное значение
            logging.info(f"The query return {one_string}")
    
    
    is_table_exists_in_db = PythonOperator(
        task_id='is_table_exists',
        python_callable=is_table_exists,
        dag=dag
    )
    
    
    start >> get_data_from_api >> is_table_exists_in_db