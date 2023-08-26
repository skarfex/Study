from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {"start_date": datetime(2022, 3, 1), "end_date": datetime(2022, 3, 14), "owner": "n-trifonov-9"}

dag = DAG("n-trifonov-9", schedule_interval="0 0 * * 1-6", default_args=DEFAULT_ARGS, tags=["n-trifonov-9"])


def fetch_heading_func(ds, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id="conn_greenplum")  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
    cursor.execute(f"SELECT heading FROM articles WHERE id = extract(isodow from date '{ ds }')")  # исполняем sql
    query_res = cursor.fetchall()[0]  # полный результат
    logging.info(query_res)


fetch_heading = PythonOperator(
    task_id="fetch_heading", python_callable=fetch_heading_func, provide_context=True, dag=dag
)

fetch_heading
