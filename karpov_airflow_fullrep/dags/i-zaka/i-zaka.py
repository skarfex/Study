import pandas as pd

from airflow import DAG
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import logging

DEFAULT_ARGS = {
    'owner': 'i-zaka',
    # 'data_interval_start': datetime(2022, 3, 1),
    # 'data_interval_end': datetime(2022, 3, 14),
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
}


def select_greenlum(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
    execution_date = str(kwargs['logical_date'])[:10]
    dow = str(pd.to_datetime(execution_date, yearfirst=True).weekday() + 1)
    select_ = rf"SELECT heading FROM articles WHERE id = {dow}"
    logging.info(execution_date)
    logging.info(select_)
    cursor.execute(select_)
    query_res = cursor.fetchall()
    logging.info(query_res)


with DAG(
    dag_id='i-zaka3',
    schedule_interval='15 21 * * 1-6',
    default_args=DEFAULT_ARGS,
    # max_active_runs=1,
    tags=['i-zaka'],
    catchup=True
) as dag:
    bash_ = BashOperator(
        task_id='test',
        bash_command='echo hello world {{ ds }}'
    )
    python_task = PythonOperator(
        task_id='select_greenplum',
        python_callable=select_greenlum
    )

bash_ >> python_task







