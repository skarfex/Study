"""
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
import logging

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime

DEFAULT_ARGS = {
    'owner': 'a-arifulin',
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'poke_interval': 600
}

with DAG("a-arifulin_2",
    schedule_interval='0 3 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags = ['a-arifulin']
) as dag:
    def get_from_greenplum(**kwargs):
        logging.info(kwargs['ds'])
        weekday = datetime.strptime(kwargs['ds'], '%Y-%m-%d').weekday() + 1
        logging.info(str(weekday))
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        logging.info(query_res[0])

    get_from_greenplum = PythonOperator(
        task_id='get_from_greenplum',
        python_callable=get_from_greenplum,
        provide_context=True
    )
