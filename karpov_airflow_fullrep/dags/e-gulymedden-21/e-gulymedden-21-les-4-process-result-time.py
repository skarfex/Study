"""
Урок 4
СЛОЖНЫЕ ПАЙПЛАЙНЫ, ЧАСТЬ 2.
Задание
"""
from airflow import DAG
import logging
from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    'owner': 'e-gulymedden-21',
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'provide_context': True
}

with DAG("e-gulymedden-21-les-4-process-result-time",
         schedule_interval='0 6 * * MON-SAT',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         catchup=True,
         tags=['e-gulymedden-21']
         ) as dag:

    dummy = DummyOperator(task_id='start_process')

    def select_query_from_gp_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum') # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor('greenplum_cursor')

        dict_week_day = {
            1: 'Monday',
            2: 'Tuesday',
            3: 'Wednesday',
            4: 'Thursday',
            5: 'Friday',
            6: 'Saturday',
            7: 'Sunday'
        }

        cursor.execute(f"SELECT heading FROM articles WHERE id = {datetime.isoweekday(datetime.strptime(kwargs['ds'], '%Y-%m-%d'))}")  # исполняем sql
        query_res = cursor.fetchall()[0]
        logging.info(
            f"execution_date = {kwargs['ds']}"
            f"weekday(number) = {datetime.isoweekday(datetime.strptime(kwargs['ds'], '%Y-%m-%d'))}"
            f"weekday(description) = {dict_week_day[datetime.isoweekday(datetime.strptime(kwargs['ds'], '%Y-%m-%d'))]}"
            f"Query starts from = {query_res}")


    select_query_from_gp = PythonOperator(
        task_id='select_query_from_gp',
        python_callable=select_query_from_gp_func
    )

    dummy >> select_query_from_gp
