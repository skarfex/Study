"""
4 урок Забирать значения из таблицы articles
"""

import pendulum

from datetime import timedelta
from airflow import DAG
import logging
from datetime import datetime

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import ShortCircuitOperator


DEFAULT_ARGS = {
    'owner': 's-markevich',
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': True,
    'poke_interval': 25,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def if_not_sunday(date):
    return not 7 == datetime.strptime(date, '%Y-%m-%d').isoweekday()


def get_from_article(date):
    pg_hook = PostgresHook('conn_greenplum')
    day_num = datetime.strptime(date, '%Y-%m-%d').isoweekday()

    logging.info(f'date: {date}')
    logging.info(f'day_num: {day_num}')

    with pg_hook.get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_num}')
        select_response = cursor.fetchone()[0]

    logging.info(f'select_response: {select_response}')


with DAG("s-markevich-lesson-4",
         start_date=pendulum.datetime(2022, 3, 1, tz='UTC'),
         end_date=pendulum.datetime(2022, 3, 14, tz='UTC'),
         schedule_interval='0 0 * * *',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['s-markevich'],
         ) as dag:

    check_sunday = ShortCircuitOperator(
        task_id='check_sunday',
        op_kwargs={'date': '{{ ds }}'},
        python_callable=if_not_sunday,
    )

    get_heading = PythonOperator(
        task_id='get_heading',
        op_kwargs={'date': '{{ ds }}'},
        python_callable=get_from_article,
    )

    check_sunday >> get_heading
