"""
Читаем данные из Greenplum
"""

from airflow import DAG
import logging
import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

import datetime

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1, tz='Europe/Moscow'),
    'end_date': pendulum.datetime(2022, 6, 30, tz='Europe/Moscow'),
    'owner': 'a-minjajlov',
    'poke_interval': 60
}

with DAG("get_table_data_01",
         schedule_interval='2 1 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-minjajlov'],
         catchup=False
         ) as dag:

    def get_weekday_num():
        weekday_num = datetime.datetime.today().isoweekday()  # Mon=1, Sun=7
        return weekday_num

    def explicit_push_func(**kwargs):
        kwargs['ti'].xcom_push(value='Hello world!', key='hi')

    def implicit_push_func():
        return 'Some string from function'

    def logging_func_start():
        logging.info("dag started")

    def logging_func_end():
        logging.info("dag end")

    def greenplum_connect(**kwargs):
        weekday_number = kwargs['ti'].xcom_pull(task_ids='calc_weekday')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        c = conn.cursor("my_cursor")
        c.execute(f'SELECT heading FROM articles WHERE id = {weekday_number}')
        result = c.fetchall()
        res = kwargs['ti'].xcom_push(value=result, key='article')
        logging.info(res)

    start_operator = PythonOperator(
        task_id='start_operator',
        python_callable=logging_func_start
    )

    calc_weekday = PythonOperator(
        task_id='calc_weekday',
        python_callable=get_weekday_num
    )

    get_data = PythonOperator(
        task_id='get_data',
        python_callable=greenplum_connect
    )

    end_operator = PythonOperator(
        task_id='end_operator',
        python_callable=logging_func_end
    )

    start_operator >> calc_weekday >> get_data >> end_operator
