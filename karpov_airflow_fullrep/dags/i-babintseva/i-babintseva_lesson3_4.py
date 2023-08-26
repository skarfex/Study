"""
This DAG extract articles from GreenPlum by id = week day (1-Mon...6-Sat)
"""
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pendulum

local_tz = pendulum.timezone("UTC")

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1, tz=local_tz),
    'end_date': pendulum.datetime(2022, 3, 14, tz=local_tz),
    'owner': 'i-babintseva'
}

with DAG('i-babintseva_lesson3_4',
         schedule_interval='0 0 * * 1-6',
         max_active_runs=1,
         default_args=DEFAULT_ARGS,
         tags=['i-babintseva']
         ) as dag:
    dummy = DummyOperator(task_id="dummy")

    echo_date = BashOperator(
        task_id='echo_date',
        bash_command='echo {{ ds }}'
    )

    def print_date_func(**kwargs):
        logging.info(kwargs['ds'])

    print_date = PythonOperator(
        task_id='print_date',
        python_callable=print_date_func,
        provide_context=True
    )

    def get_articles_func(**kwargs):
        day = datetime.strptime(kwargs['ds'], '%Y-%m-%d').weekday() + 1

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day}')
        query_res = cursor.fetchone()[0]
        logging.info(f'Article heading by id {day}: {query_res}')

    get_articles_from_gp = PythonOperator(
        task_id='get_articles_from_gp',
        python_callable=get_articles_func,
        provide_context=True
    )

    dummy >> [echo_date, print_date] >> get_articles_from_gp
