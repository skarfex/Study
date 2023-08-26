"""
4 УРОК

СЛОЖНЫЕ ПАЙПЛАЙНЫ, ЧАСТЬ 2
"""
import xml.etree.ElementTree as ET
import logging

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'owner': 'y_niyazbayev',
    'poke_interval': 600
}


with DAG("y_niyazbayev_homework_4",
          schedule_interval='0 0 * * 1-6',
          catchup=True,
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['y_niyazbayev']
          ) as dag:

    echo_start = BashOperator(
        task_id='echo_start',
        bash_command='echo {{ ts }}\necho day of week: {{ execution_date.isoweekday() + 1}}\necho started',
        dag=dag
    )


    def get_heading_from_articles_func(weekday):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')
        query_res = cursor.fetchall()
        logging.info(query_res)


    get_heading_from_articles = PythonOperator(
        task_id='get_heading_from_articles_func',
        python_callable=get_heading_from_articles_func,
        op_args=['{{ execution_date.isoweekday() + 1}}']
    )

    echo_finish = BashOperator(
        task_id='echo_finish',
        bash_command='echo finished',
        dag=dag
    )

echo_start >> get_heading_from_articles >> echo_finish
