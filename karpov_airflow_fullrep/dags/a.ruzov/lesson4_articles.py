"""
Опсание DAG'а
Работать с понедельника по субботу, но не по воскресеньям

Ходить в наш GreenPlum.

Использовать соединение 'conn_greenplum'

Забирать из таблицы articles значение поля heading из строки с id, равным дню недели ds
(понедельник=1, вторник=2, ...)
Выводить результат работы в любом виде: в логах либо в XCom'е
Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года
"""

from datetime import datetime

from airflow import DAG
import pendulum
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

from airflow import DAG
import pendulum
import logging

from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1, tz='utc'),
    'end_date': pendulum.datetime(2022, 3, 14, tz='utc'),
    'owner': 'a.ruzov',
    'email': ['ruzov.anton@yandex.ru'],
}


with DAG(
    dag_id='a.ruzov_atg_articles',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a.ruzov_get_articles']
) as dag:
    def get_week_day(str_date):
        return pendulum.from_format(str_date, 'YYYY-MM-DD').weekday() + 1


    def is_not_sunday_func(**context):
        return get_week_day(context['ds']) != 7


    def get_article_func(**context):
        week_day = get_week_day(context['ds'])
        get_article_sql = f"SELECT heading FROM articles WHERE id = {week_day}"

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(get_article_sql)
        query_res = cursor.fetchone()
        logging.info(query_res[0])


    is_not_sunday = ShortCircuitOperator(
        task_id='is_not_sunday',
        python_callable=is_not_sunday_func
    )

    get_article = PythonOperator(
        task_id='get_article',
        python_callable=get_article_func
    )

    is_not_sunday >> get_article

