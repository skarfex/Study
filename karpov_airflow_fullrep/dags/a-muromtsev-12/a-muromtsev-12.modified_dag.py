"""
A Dag that pulls data from GreenPlum
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

import logging

DEFAULT_ARGS = {
    'owner': 'a-muromtsev-12',
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'retries': 3,
    'poke_interval': 600
}


def pull_heading_func(exec_day, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor("named_cursor_name")
    cursor.execute(f'SELECT heading FROM articles WHERE id = {exec_day}')
    query_res = cursor.fetchall()
    kwargs['ti'].xcom_push(value=query_res, key='article_head')  # в XCOM
    logging.info(f"{query_res}")  # в лог
    return


with DAG(
        dag_id="a-muromtsev-12.modified_dag",
        schedule_interval='0 12 * 3 MON-SAT',
        catchup=True,
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['a-muromtsev-12']
) as dag:
    start = DummyOperator(task_id='start')

    pull_heading = PythonOperator(
        task_id='pull_heading',
        python_callable=pull_heading_func,
        op_args=['{{dag_run.logical_date.weekday() + 1 }}', ],
        provide_context=True
    )

    start >> pull_heading
