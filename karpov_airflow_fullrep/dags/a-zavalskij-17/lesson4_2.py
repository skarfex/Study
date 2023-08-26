"""
Получаем заголовки статей из GP
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'owner': 'a-zavalskij-17',
    'poke_interval': 600,
}


with DAG(
    "a-zavalskij-17-articles",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-zavalskij-17'],
) as dag:

    start = DummyOperator(task_id='start')

    def get_article_headers_func(ds, **kwargs):
        logging.info(f'{ds}')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor(
            "named_cursor_name"
        )  # и именованный (необязательно) курсор
        weekday = datetime.strptime(ds, '%Y-%m-%d').isoweekday()
        cursor.execute(
            f'SELECT heading FROM articles WHERE id = {weekday}'
        )  # исполняем sql
        query_res = cursor.fetchone()[0]  # если вернулось единственное значение
        conn.close()
        logging.info(f'{query_res}')
        kwargs['ti'].xcom_push(value=query_res, key='header')

    get_article_headers = PythonOperator(
        task_id='get_article_headers', python_callable=get_article_headers_func, dag=dag
    )

    start >> get_article_headers