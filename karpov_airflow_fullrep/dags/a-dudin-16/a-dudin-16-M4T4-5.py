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

from airflow import DAG
from datetime import datetime
import logging
import locale
import pendulum
locale.setlocale(locale.LC_ALL, '')

from airflow.hooks.postgres_hook import PostgresHook  # a hook for greenplum connection
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator

DEFAULT_ARGS = {
    'owner': 'a-dudin-16',
    'start_date': pendulum.datetime(2022, 3, 1, tz='utc'),
    'end_date': pendulum.datetime(2022, 3, 15, tz='utc'),
    'poke_interval': 90
    }

with DAG("a-dudin-16-5",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-dudin-16']
    ) as dag:

    def if_execution_day(ds):
        execution_day = datetime.strptime(ds, "%Y-%m-%d").isoweekday()
        if execution_day in [1, 2, 3, 4, 5, 6]:
            return 'select_from_table'
        else:
            return 'sunday'

    sunday_branch = BranchPythonOperator(
        task_id='sunday_branch',
        python_callable=if_execution_day,
        dag=dag
        )

    def select_from_greenplum(ds):
        execution_day = datetime.strptime(ds,"%Y-%m-%d").isoweekday()
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()  # get a connection from the hook
        cursor = conn.cursor()  # get a cursor from the hook
        sql_select = f'SELECT heading FROM articles WHERE id = {execution_day}'
        cursor.execute(sql_select)  # sql select
        one_string = cursor.fetchone()[0]  # single value been returned
        logging.info('*****  DATABASE STRING ****')
        logging.info(one_string)
        logging.info('*********')
        logging.info(f'WEEKDAY: {execution_day}')
        logging.info('*********')

    select_from_table = PythonOperator(
        task_id='select_from_table',
        python_callable=select_from_greenplum,
        dag=dag
        )

    def tis_sunday():
        print("This is Sunday")

    sunday = PythonOperator(
        task_id='sunday',
        python_callable=tis_sunday
        )

sunday_branch >> [select_from_table, sunday]
