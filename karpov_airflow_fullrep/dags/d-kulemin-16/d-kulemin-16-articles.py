"""
Даг, который возвращает article с id равным дню недели.
"""

from airflow import DAG
import datetime as dt
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': dt.datetime(2022, 3, 1),
    'end_date': dt.datetime(2022, 3, 14),
    'owner': 'd-kulemin-16',
    'poke_interval': 600
}

with DAG(
    "d-kulemin-16-articles",
    schedule_interval='0 0 * * *',
    default_args=DEFAULT_ARGS,
    max_active_runs=8,
    tags=['d-kulemin-16']
) as dag:
    start = DummyOperator(task_id='start')

    def is_day_for_articles(**kwargs):
        execution_dt = kwargs['execution_dt']
        exec_day = dt.datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return exec_day != 6

    day_for_articles = ShortCircuitOperator(
        task_id='day_for_articles',
        python_callable=is_day_for_articles,
        op_kwargs={'execution_dt': '{{ ds }}'}
    )

    def get_article_by_day(**kwargs):
        execution_dt = kwargs['execution_dt']
        exec_day = dt.datetime.strptime(execution_dt, '%Y-%m-%d').weekday() + 1

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {exec_day}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат

        logging.info('=========================')
        logging.info(f'day num is {exec_day}, and dt is {execution_dt}')
        logging.info(query_res)
        logging.info('=========================')

    get_article = PythonOperator(
        task_id='get_article',
        python_callable=get_article_by_day,
        op_kwargs={'execution_dt': '{{ ds }}'}
    )

    start >> day_for_articles >> get_article
