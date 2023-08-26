"""
Даг: Забирать из таблицы articles значение поля heading из строки с id,
равным дню недели ds (понедельник=1, вторник=2, ...)
"""
from airflow import DAG
import logging
from datetime import datetime, timedelta

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 't-velinetskaja',
    'poke_interval': 600
}

with DAG("tvel_less2_4_hw",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         catchup=True,
         tags=['velinetskaja', 'less2_4']
         ) as dag:

    def no_sunday_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        logging.info (exec_day)
        return exec_day != 6

    no_sunday = ShortCircuitOperator(
        task_id='no_sunday',
        python_callable=no_sunday_func,
        op_kwargs={'execution_dt': '{{ ds }}'}
        )

    def data_from_greenplum_func(execution_dt):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d')
        logging.info(exec_day)
        exec_day_wd = exec_day.weekday()
        logging.info(exec_day_wd)
        cursor.execute(f'SELECT heading FROM articles WHERE id = {exec_day_wd}')  # исполняем sql
        query_res = cursor.fetchone()
        res = query_res[0]
        logging.info(exec_day + " " + res)

    data_from_greenplum = PythonOperator(
        task_id='data_from_greenplum',
        python_callable=data_from_greenplum_func,
        op_kwargs={'execution_dt': '{{ tomorrow_ds }}'}
    )


no_sunday >> data_from_greenplum