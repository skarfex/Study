"""
> Задание
Нужно доработать даг, который вы создали на прошлом занятии.
Он должен:
Работать с понедельника по субботу, но не по воскресеньям (можно реализовать с помощью расписания или операторов ветвления)
Ходить в наш GreenPlum. Вариант решения — PythonOperator с PostgresHook внутри
Используйте соединение 'conn_greenplum' в случае, если вы работаете из LMS либо настройте его самостоятельно в вашем личном Airflow. Параметры соединения:

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

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'svreutov',
    'poke_interval': 600
}

with DAG ("s-reutov-16_lesson_4",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          catchup=True,
          tags=['svreutov']
          ) as dag:

    start = DummyOperator(
        task_id='start'
    )

    def load_from_gp_func(**kwargs):
        # инициализируем хук
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        # берём из него соединение
        conn = pg_hook.get_conn()
        # и именованный (необязательно) курсор
        cursor = conn.cursor("named_cursor_name")
        # исполняем sql
        cursor.execute("SELECT heading FROM articles WHERE id =" + kwargs['isoweek'])
        # полный результат
        query_res = cursor.fetchall()
        # если вернулось единственное значение
        #one_string = cursor.fetchone()[0]
        logging.info(40 * '-')
        logging.info(query_res)
        logging.info(40 * '-')


    load_from_gp = PythonOperator(
        task_id='load_from_gp',
        python_callable=load_from_gp_func,
        op_kwargs={'isoweek': '{{ execution_date.isoweekday() }}' }
    )

    start >> load_from_gp
