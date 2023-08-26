"""
a-martyshkin-15
Урок 4 - принты из базы
"""
from airflow import DAG
from datetime import datetime
import locale
import pendulum
import logging

import datetime as dt

from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook #  хук для работы с GP
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'owner': 'a-martyshkin-15',
    'start_date': pendulum.datetime(2022, 3, 1, tz='utc'),
    'end_date': pendulum.datetime(2022, 3, 14, tz='utc'),
    'poke_interval': 120
}

with DAG('martyshkin_lesson_4',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-martyshkin-15']
        ) as dag:

    def get_week_day(ds):
        return pendulum.from_format(ds, 'YYYY-MM-DD').isoweekday()


    def is_not_sunday_func(ds):
        return get_week_day(ds) != 7


    def get_article_heading_func(ds):
        week_day = get_week_day(ds)
        get_article_sql = f"SELECT heading FROM articles WHERE id = {week_day}"
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(get_article_sql)
        query_res = cursor.fetchone()
        logging.info(query_res[0])


    is_not_sunday = ShortCircuitOperator(
        task_id='is_not_sunday',
        python_callable=is_not_sunday_func
    )

    get_article_heading = PythonOperator(
        task_id='get_article_heading',
        python_callable=get_article_heading_func
    )


    is_not_sunday >> get_article_heading
