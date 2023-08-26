"""
l4
"""

import logging
from airflow import DAG
from airflow.utils.dates import days_ago
import datetime

from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime.datetime(2022, 3, 2),
    'end_date': datetime.datetime(2022, 3, 15),
    'owner': 'ds',
    'poke_interval': 600
}

with DAG(
        dag_id='a-sokolov-6-lesson-4-extra',
        #description="Даг для 4го урока",
        default_args=DEFAULT_ARGS,
        schedule_interval='20 1 * * 1-6',  # at 1:20am /*day of month /*month / from Sunday=0 Mon=1...to Saturday=6
        max_active_runs=1,
        tags=['ais']
) as dag:


    def get_gp_cursor():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("gp_conn")
        return cursor


    def get_article_by_id(article_id):
        logging.info(f'Executing get_article_by_id with article_id={article_id}')
        cursor = get_gp_cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {article_id};')
        result = cursor.fetchone()[0]
        logging.info(f'Article headding: {result}')


    get_article = PythonOperator(
        task_id='get_article_by_id',
        python_callable=get_article_by_id,
        op_args=['{{ dag_run.logical_date.weekday() + 1 }}', ],
        do_xcom_push=True,
        dag=dag
    )

    get_article