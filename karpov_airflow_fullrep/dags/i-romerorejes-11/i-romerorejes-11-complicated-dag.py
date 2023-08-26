"""
Это даг, который должен Работать с понедельника по субботу, но не по воскресеньям
(можно реализовать с помощью расписания или операторов ветвления)
Забирать из таблицы articles значение поля heading из строки с id, равным дню недели ds (понедельник=1, вторник=2, ...)
Выводить результат работы в любом виде: в логах либо в XCom'е
"""
from datetime import datetime

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook # c помощью этого hook будем входить в наш Greenplan
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import  PythonOperator
import pendulum
import logging

DEFAULT_ARGS = {
#    'start_date': pendulum.datetime(2022, 3, 2),
#    'end_day': pendulum.datetime(2022,3,15),
#    'start_day': days_ago(40),
    'owner': 'i-romerorejes-11',
    'poke_interval': 600,
#    'catchup':True
}



def greenplum_query_func(ds):
    query = f"SELECT heading FROM articles WHERE id = {datetime.strptime(ds, '%Y-%m-%d').isoweekday()}"
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
    cursor.execute(query)  # исполняем sql
    query_res = cursor.fetchall()  # полный результат
    for item in query_res:
        logging.info(f'Название заголовка: {item[0]}')  # выводим результат работы в логи


def data_log_func(ds):
    log_value = f"Дата: {ds}, День недели: {datetime.strptime(ds, '%Y-%m-%d').isoweekday()}"
    logging.info(log_value)


with DAG("i_romerorejes_11_plugins-compicated-dag",
# взят формат cron и собран на сайте https://crontab.guru/#9_0_1-14_3_1-6
          schedule_interval='0 0 * * 1-6',
#          data_interval_start = 2022-03-01,
#          data_interval_end = 2022-03-14,
          start_date = pendulum.datetime(2022, 3, 1),
          default_args=DEFAULT_ARGS,
          max_active_runs=1, # это означает что в единственный момент времени запущен только один даг
          tags=['i_romerorejes_11']
          ) as dag:

        dummy = DummyOperator(task_id='dummy')

        greenplum_query = PythonOperator(
            task_id='greenplum_query',
            python_callable = greenplum_query_func,
            dag=dag
        )

        data_log = PythonOperator(
            task_id = 'data_log',
            python_callable = data_log_func,
            dag = dag
        )

        dummy >> greenplum_query >> data_log
