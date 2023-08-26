import pendulum
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1, tz="UTC"),
    'end_date': pendulum.datetime(2022, 3, 14, tz="UTC"),
    'owner': 'a-grishin'
}


def get_head_func(day_of_week, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')
    query_res = cursor.fetchone()[0]
    logging.info(query_res)


with DAG("a-grishin-greenplum",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-grishin']) as dag:
    get_articles_head = PythonOperator(
        task_id='get_articles_heading',
        python_callable=get_head_func,
        op_args=['{{ logical_date.weekday() + 1 }}']
    )

    get_articles_head
