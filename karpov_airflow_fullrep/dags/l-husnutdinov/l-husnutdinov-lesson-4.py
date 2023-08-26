# -*- coding: utf-8 -*-
"""
Greenplum DAG
"""

from datetime import datetime
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import DAG
from airflow.decorators import task


DEFAULT_ARGS = {
    'start_date': datetime(year=2022, month=3, day=1),
    'end_date': datetime(year=2022, month=3, day=14),
    'owner': 'l-husnutdinov',
    'poke_interval': 600
}

with DAG(
        dag_id='l-husnutdinov-lesson-4',
        schedule_interval='0 0 * * 1-6',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['lesson-4', 'l-husnutdinov']
) as dag:
    @task(task_id="pythonFetchOp")
    def fetch_func(week_day: str):
        week_day_id = datetime.strptime(week_day, '%Y-%m-%d').isoweekday()
        logging.info(f"Id of the week day: {week_day_id}")

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f"SELECT heading FROM articles WHERE id = {week_day_id}")
        query_res = cursor.fetchone()[0]
        conn.close()

        logging.info(f'Text of the article: {query_res}')

    pythonFetchOp = fetch_func("{{ ds }}")

    pythonFetchOp
