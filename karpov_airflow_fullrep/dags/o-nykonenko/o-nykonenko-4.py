"""
Работает с понедельника по субботу
Ходит в GreenPlum. Вариант решения — PythonOperator с PostgresHook внутри
Использует соединение 'conn_greenplum'
Забирает из таблицы articles значение поля heading из строки с id, равным дню недели ds (понедельник=1, вторник=2, ...)
Выводит результат работы в любом виде: в логах либо в XCom'е
Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года
"""
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'owner': 'o-nykonenko',
    'poke_interval': 600
}

with DAG(
    dag_id= "o-nykonenko-4",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['o-nykonenko']
) as dag:

    def not_sunday_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return exec_day!=6


    not_sunday = ShortCircuitOperator(
        task_id='not_sunday',
        python_callable=not_sunday_func,
        op_kwargs={'execution_dt': '{{ds}}'}
                   )

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

    not_sunday >> get_from_greenplum