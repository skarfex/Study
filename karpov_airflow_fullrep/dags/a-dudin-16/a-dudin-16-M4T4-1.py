"""
Работать с понедельника по субботу, но не по воскресеньям
(можно реализовать с помощью расписания или операторов ветвления)
Ходить в наш GreenPlum. Вариант решения — PythonOperator с PostgresHook внутри
Используйте соединение 'conn_greenplum' в случае, если вы работаете из LMS либо настройте его самостоятельно в вашем
личном Airflow. Параметры соединения:
Host: greenplum.lab.karpov.courses
Port: 6432
DataBase: karpovcourses
Login: student
Password: Wrhy96_09iPcreqAS
Забирать из таблицы articles значение поля heading из строки с id, равным дню недели ds (понедельник=1, вторник=2, ...)
Выводить результат работы в любом виде: в логах либо в XCom'е
Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года
"""
# не работает, выцепляет только 1 марта

from airflow.hooks.postgres_hook import PostgresHook
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, date
import csv
import logging

DEFAULT_ARGS = {
    'owner': 'a-dudin-16',
    'start_date': datetime(2022,3,1),
    'end_date': datetime(2022,3,15),
    'poke_interval': 90
}

with DAG("a-dudin-16-1",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-dudin-16'],
         catchup=True,
         ) as dag:

    def select_from_greenplum(current_day):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        weekday = date.fromisoformat(current_day).weekday() + 1
        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')
        query_res = cursor.fetchall()
        logging.info(f"current_day {current_day}")
        logging.info(f"weekday {weekday}")
        logging.info(f"query_res {query_res}")

    load_data = PythonOperator(
        task_id='select_from_greenplum',
        python_callable=select_from_greenplum,
        op_args=['{{ ds }}']
    )

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    start >> load_data >> end
