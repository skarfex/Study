"""
DAG должен:

Работать с понедельника по субботу, но не по воскресеньям

Ходить в наш GreenPlum.

Используйте соединение 'conn_greenplum' в случае, если вы работаете из LMS

Забирать из таблицы articles значение поля heading из строки с id, равным дню недели ds
(понедельник=1, вторник=2, ...)
Выводить результат работы в любом виде: в логах либо в XCom'е
Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года
"""
from datetime import datetime

from airflow import DAG
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'owner': 'a-tagirov-15',
    'poke_interval': 600
}


with DAG(
        dag_id='atg_articles',
        schedule_interval='0 3 * * 1-6',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        catchup=True,
        tags=['atg']
) as dag:


    def from_gp_func(day):
        logging.info('????????????????????')
        logging.info('It is from_gp_func')
        logging.info('????????????????????')

        num_day_of_the_week = datetime.strptime(day, '%Y-%m-%d').isoweekday()

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()

        cursor = conn.cursor('get_articles')
        sql_request = f'SELECT heading FROM articles WHERE id={num_day_of_the_week}'
        cursor.execute(sql_request)
        query_res = cursor.fetchall()

        logging.info('????????????????????')
        logging.info(query_res)
        logging.info(f'today: {day}')
        logging.info(f'mum_day: {num_day_of_the_week}')
        logging.info('????????????????????')


    from_gp = PythonOperator(
        task_id='from_gp',
        python_callable=from_gp_func,
        op_args=['{{ ds }}']
    )

    from_gp