"""
Доработать даг, он должен:

Работать с понедельника по субботу, но не по воскресеньям (можно реализовать с помощью расписания или операторов ветвления)

Ходить в наш GreenPlum. Вариант решения — PythonOperator с PostgresHook внутри

Забирать из таблицы articles значение поля heading из строки с id, равным дню недели ds (понедельник=1, вторник=2, ...)

Выводить результат работы в любом виде: в логах либо в XCom'е
Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года
"""

from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    # 'start_date': days_ago(7),
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'owner': 's-jushanov-9',
    'poke_interval': 600,
    'catchup': False
}


with DAG(
    "s_jushanov_02_04_task",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['s-jushanov-9']
) as dag:

    start_task = DummyOperator(
        task_id='start_task'
    )

    def load_articles_by_weekday_func(execution_date, **kwargs):
        weekday_id = execution_date.weekday() + 1
        pg_hook = PostgresHook('conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday_id}')
        one_string = cursor.fetchone()[0]
        logging.info(f'{execution_date.date()} : {one_string}')

    load_articles_by_weekday = PythonOperator(
        task_id='load_articles_by_weekday',
        python_callable=load_articles_by_weekday_func,
        provide_context=True
    )

    start_task >> load_articles_by_weekday
