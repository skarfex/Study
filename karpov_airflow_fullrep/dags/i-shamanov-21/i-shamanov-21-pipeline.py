"""
Пайплайн для урока ETL #4 (запросы из GreenPlum)
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from datetime import date, datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'i-shamanov-21',
    'poke_interval': 600
}

with DAG('i-shamanov-21-reset-with-catchup',
    schedule_interval='10 10 * * 1-6',
    catchup=True,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['i-shamanov-21']
) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    def get_article_heading_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor('i-shamanov-21')
        run_date = date.fromisoformat(kwargs['ds']).isoweekday()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {run_date}')
        logging.info(cursor.fetchone()[0])

    get_article_heading = PythonOperator(
        task_id='get_article_heading',
        python_callable=get_article_heading_func,
        provide_context=True,
        dag=dag
    )

    start >> get_article_heading >> end
