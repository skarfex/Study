"""
dag for extractig articles from greenplum and logging them
"""

import logging
from datetime import datetime, date
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1, tz='utc'),
    'end_date': pendulum.datetime(2022, 3, 14, tz='utc'),
    # start and end dates were specified using pendulum module for easy time zone interpretation
    'owner': 'm-babukhin',
    'poke_interval': 600
}

dag = DAG("babukhin_get_articles",
          schedule_interval='0 0 * * 1-6',
          # schedule interval specified using cron expression: every day except Sunday at 00:00 UTC
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['m-babukhin']
          )


def load_data_from_gp(ds):
    """
    function that connects to GP using PostgresHook, 
    extracts articles as per execution date number and logging them
    """
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    with conn.cursor() as cursor:
        week_day_number = datetime.isoweekday(date.fromisoformat(ds))
        # ds variable returns str, we need to format it as date
        logging.info(ds)
        cursor.execute(
            f'SELECT heading FROM articles WHERE id = {week_day_number}')
        # return article with id equals to week day number (Mon=1,... Sun=7)
        query_res = cursor.fetchall()
        logging.info(query_res)
        # return article to airflow log


load_data_from_gp = PythonOperator(
    task_id='load_data_from_gp',
    python_callable=load_data_from_gp,
    dag=dag
)

load_data_from_gp
