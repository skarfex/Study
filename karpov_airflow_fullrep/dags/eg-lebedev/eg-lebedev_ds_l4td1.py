"""
Задание 1
"""
from airflow import DAG, macros
import logging
import datetime

from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime.datetime(2022, 3, 1),
    'end_date': datetime.datetime(2022, 3, 14),
    'owner': 'eg-lebedev',
    'poke_interval': 60
}

def extract(**kwargs):
    greenplum_hook = PostgresHook('conn_greenplum')
    conn = greenplum_hook.get_conn()
    cursor = conn.cursor('some_cursor')
    logging.info(kwargs)
    logging.info("week day of {} is {}".format(kwargs['logical_date'], kwargs['logical_date'].weekday() + 1))
    cursor.execute("select heading from articles where id = {}".format(kwargs['logical_date'].weekday() + 1))
    res = cursor.fetchall()
    logging.info(res)

with DAG("ds_l4_v2",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=5,
    tags=['eg-lebedev']
) as dag:

    extract = PythonOperator(
        task_id='extract_article_heading',
        python_callable=extract
    )

    extract