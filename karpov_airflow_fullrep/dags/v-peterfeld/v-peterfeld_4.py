"""
4 урок
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from textwrap import dedent
from datetime import datetime
import pendulum

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'owner': 'v-peterfeld',
    'start_date': pendulum.datetime(2022, 3, 1, tz='utc'),
    'end_date': pendulum.datetime(2022, 3, 14, tz='utc'),
}

with DAG("v-peterfeld_4",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['v-peterfeld','task_4']) as dag:

    start = DummyOperator(task_id='start')

    def select_no_sunday(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()+1
        if exec_day != 7:
            return 'read_gp'
        else:
            return 'no_task'

    select_sunday = BranchPythonOperator(
        task_id='select_sunday',
        python_callable=select_no_sunday,
        op_kwargs={'execution_dt': '{{ds}}'})


    def week_day_func(execution_dt):
        return pendulum.from_format(execution_dt, 'YYYY-MM-DD').weekday() + 1

    def read_gp_func(**kwargs):
        week_day = week_day_func(kwargs['ds'])
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {week_day}')  # исполняем sql
        query_res = cursor.fetchone()
        logging.info(query_res[0])

    read_gp = PythonOperator(
        task_id='read_gp',
        python_callable=read_gp_func)

    no_task = DummyOperator(task_id='no_task')

start >> select_sunday >> [read_gp, no_task]



