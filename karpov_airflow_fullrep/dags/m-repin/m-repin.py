"""
4 урок 1 задание
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime
import pandas as pd

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 2),
    'end_date': datetime(2022, 3, 15),
    'owner': 'm-repin',
    #'depends_on_past': True,
    #'wait_for_downstream': True,
    'poke_interval': 600
}


with DAG("m_repin",
    schedule_interval='0 0 * * *',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['m_repin']
    ) as dag:


    def is_not_sunday_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return exec_day in [1, 2, 3, 4, 5, 6]

    is_not_sunday_func = ShortCircuitOperator(
        task_id='is_not_sunday_func',
        python_callable=is_not_sunday_func,
        op_kwargs={'execution_dt': '{{ ds }}'},
        dag=dag
        )
    

    def get_data_from_gp_func(execution_dt):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        day_of_week = pd.Timestamp(execution_dt).dayofweek
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        logging.info("The query look like: ")
        logging.info(f'SELECT heading FROM articles WHERE id = {day_of_week}')
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')  # исполняем sql
        try:
            query_res = cursor.fetchall()  # полный результат
            logging.info(f"The query return {query_res}")
        except:
            one_string = cursor.fetchone()[0]  # если вернулось единственное значение
            logging.info(f"The query return {one_string}")


    get_data_from_gp = PythonOperator(
        task_id='get_data_from_gp',
        python_callable=get_data_from_gp_func,
        op_kwargs={'execution_dt': '{{ ds }}'},
        dag=dag
        )

    is_not_sunday_func >> get_data_from_gp
    
    













