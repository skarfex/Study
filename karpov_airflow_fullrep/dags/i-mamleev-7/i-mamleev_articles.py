"""
Тестовый даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 2),
    'end_date': datetime(2022, 3, 15),
    'owner': 'i-mamleev-7',
    'poke_interval': 20
}

with DAG("i-mamleev-7_lesson_4",
    schedule_interval='0 0 * * 1-6',
    catchup=True,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['i-mamleev']
) as dag:


    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}'
    )

    def load_data(arg):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        week_day = datetime.strptime(arg, '%Y-%m-%d').weekday()+1
        cursor.execute(f'SELECT heading FROM articles WHERE id = {week_day}')
        query_res = cursor.fetchall()
        logging.info(query_res)


    get_article_heading = PythonOperator(
        task_id = 'get_article_heading',
        python_callable=load_data,
        op_args = ['{{ds}}']
    )

    get_article_heading