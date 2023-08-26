"""
даг - забирает из greenplum из таблицы articles значение поля heading из строки с id, равным дню недели ds
Работает с пн по сб
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime, timezone
import pendulum

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'owner': 'g-haritonova-10',
    'start_date': pendulum.datetime(2022, 3, 1, tz="UTC"),
    'end_date': pendulum.datetime(2022, 3, 14, tz="UTC"),
    'poke_interval': 600
}

with DAG("g-haritonova-10-second-dag",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['g-haritonova-10']
          ) as dag:

    dummy = DummyOperator(task_id='dummy')

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def week_day(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return exec_day

    def read_from_gp_func(execution_dt):
        weekday_num = week_day(execution_dt)+1
        if weekday_num != 7:
            pg_hook = PostgresHook('conn_greenplum')
            conn = pg_hook.get_conn()  # берём из него соединение
            cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
            cursor.execute('SELECT heading FROM articles WHERE id = ' + str(weekday_num))  # исполняем sql
            query_res = cursor.fetchall()  # полный результат
            return query_res
        else:
            return 'sunday'

    log_read_from_gp = PythonOperator(
        task_id='log_read_from_gp',
        python_callable=read_from_gp_func,
        op_kwargs={'execution_dt': '{{ ds }}'},
        dag=dag
    )

    dummy >> [echo_ds, log_read_from_gp]


