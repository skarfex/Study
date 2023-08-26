"""
Тестовый даг даг
"""
from datetime import datetime
from textwrap import dedent

from airflow import DAG

import logging
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator
import pendulum

DEFAULT_ARGS = {
    'owner': 'a-tretjakov-19',
    'poke_interval': 600,
    'start_date': pendulum.datetime(2022, 3, 1, tz='utc'),
    'end_date': pendulum.datetime(2022, 3, 14, tz='utc')
}

# sd = datetime.isoweekday(DEFAULT_ARGS['start_date'])


with DAG("tretjakov_dag",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-tretjakov-19']
         ) as dag:

    start = DummyOperator(task_id="start")

    def is_not_sunday_func(execution_dt):
        exec_day = datetime.isoweekday(datetime.strptime(execution_dt, '%Y-%m-%d'))
        return exec_day in [1, 2, 3, 4, 5, 6]

    not_sunday = ShortCircuitOperator(
        task_id='not_sunday',
        python_callable=is_not_sunday_func,
        op_kwargs={'execution_dt': '{{ ds }}'}
    )

    bashoperdata = BashOperator(
        task_id='bashoperdata',
        bash_command='echo {{ ds }}'

    )

    def date_func():
        return datetime.now()

    pythonoperdata = PythonOperator(
        task_id='pythonoperdata',
        python_callable=date_func
    )

    def get_str(execution_dt):
        sdf = datetime.strptime(execution_dt, '%Y-%m-%d').date()
        sd = datetime.isoweekday(sdf)
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {sd}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        conn.close()
        logging.info('n\Результат:', query_res)
        logging.info('n\Дата:', sdf)
        return 'Результат:' + str(query_res) + '=> Дата:' + str(sdf)

    get_str_in_bd = PythonOperator(
        task_id='get_str_in_bd',
        python_callable=get_str,
        op_kwargs={'execution_dt': '{{ ds }}'}
    )

    start >> not_sunday >> [bashoperdata, pythonoperdata] >> get_str_in_bd
