"""
test dag
"""
import datetime
import logging

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime.datetime(2022, 3, 1, 6),
    'end_date': datetime.datetime(2022, 3, 14, 6),
    'owner': 'i-pestretsov-17',
}

with DAG("i-pestretsov-17_test",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         catchup=True
         ) as dag:
    dummy = DummyOperator(task_id="dummy")


    def get_articles_func(ds):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        date_object = datetime.datetime.strptime(ds, '%Y-%m-%d').date()
        query = f'SELECT heading FROM articles WHERE id = {date_object.isoweekday()}'
        cursor.execute(query)
        query_res = cursor.fetchall()
        logging.info(query_res)


    get_articles = PythonOperator(
        task_id='get_articles',
        dag=dag,
        python_callable=get_articles_func,

    )

    dummy >> get_articles
