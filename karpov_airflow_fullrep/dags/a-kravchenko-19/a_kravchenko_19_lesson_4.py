"""
Складываем курс валют в GreenPlum
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
import logging
import csv
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'a-kravchenko-19',
    'poke_interval': 600
}


def print_gp_data():
    dw = datetime.now().isoweekday()
    logging.info(f'Current weekday: {dw}')
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
    cursor.execute(f'SELECT heading FROM articles WHERE id = {dw}')  # исполняем sql
    query_res = cursor.fetchall()  # полный результат
    for r in query_res:
        print(r)


with DAG("a-kravchenko-19-lesson-4",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-kravchenko-19']
         ) as dag:
    start = DummyOperator(task_id='start', dag=dag)
    end = DummyOperator(task_id='end', dag=dag)
    python_gp_data_print = PythonOperator(task_id='python_gp_data_print', python_callable=print_gp_data)
    start >> python_gp_data_print >> end

