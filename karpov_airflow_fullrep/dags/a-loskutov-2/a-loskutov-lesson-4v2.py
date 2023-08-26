"""
Dag for forth lesson,
where we want to get an article of table
using cursor and PostgresHook.
"""
import airflow
import locale
import logging
import pendulum
locale.setlocale(locale.LC_ALL, '')
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

DEFAULT_ARGS = {
    'owner': 'a-loskutov-2',
    'start_date': pendulum.datetime(2022, 3, 1, tz='utc'),
    'end_date': pendulum.datetime(2022, 3, 14, tz='utc')
}

dag = DAG("a-loskutov-lesson-4v2",
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['Loskutov'],
          schedule_interval='0 0 * * MON-SAT'
          )


def get_sql_def(ds):
    numb_day = datetime.strptime(ds, "%Y-%m-%d").isoweekday()
    get_sql = f'SELECT heading FROM articles WHERE id = {numb_day}'

    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("cursor_name")  # и именованный (необязательно) курсор
    cursor.execute(get_sql)  # исполняем sql
    #query_res = cursor.fetchall()  # полный результат
    one_string = cursor.fetchone()[0]  # если вернулось единственное значение

    logging.info(f'today: {numb_day}')
    logging.info(f'first string: {one_string}')


return_result = PythonOperator(
    task_id='return_result',
    python_callable=get_sql_def,
    dag=dag
)

return_result