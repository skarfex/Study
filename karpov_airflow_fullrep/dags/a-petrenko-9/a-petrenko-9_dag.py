"""
DAG для получения данных из таблицы с фильтром по дням недели и вывод их в лог
"""

from airflow.decorators import dag, task
import logging

import pendulum
import datetime
from airflow.hooks.postgres_hook import PostgresHook


DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1, tz="UTC"),
    'end_date': pendulum.datetime(2022, 3, 14, tz="UTC"),
    'owner': 'a-petrenko-9',
    'poke_interval': 600
}

@dag(
    dag_id="a-petrenko-9_simple_dag",
    start_date=pendulum.datetime(2022, 3, 1, tz="UTC"),
    end_date=pendulum.datetime(2022, 3, 14, tz="UTC"),
    schedule_interval='0 0 * * MON-SAT',
    catchup=True,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['ap-9']
)
def using_dag_flow():

    @task()
    def get_articles_data_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        date = kwargs['ds']
        logging.info('DS date = ', date, 'TYPE - ', type(date))
        year, month, day = date.split('-')
        logging.info('year = ', year, 'month =', month, 'day =', day)
        day_of_week = datetime.date(int(year), int(month), int(day)).isoweekday()
        logging.info('day_of_week = ', day_of_week)
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')
        query_res = cursor.fetchall()
        logging.info('FETCH ALL = ', query_res)

    get_articles_data_func()


dag = using_dag_flow()