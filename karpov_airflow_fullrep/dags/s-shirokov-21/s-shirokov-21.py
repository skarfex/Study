"""
Простой даг
"""
import logging
from airflow import DAG
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'owner': 's-shirokov-21',
    'retries': 1,
    'poke_interval': 600,
    'retry_delay': timedelta(seconds=15)
}

with DAG('s-shirokov-21',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         schedule_interval='0 0 * * 1-6',
         tags=['s-shirokov-21'],
         catchup=True) as dag:

    start = DummyOperator(task_id="start")

    def get_heading(weekday):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        query = f'SELECT heading FROM articles WHERE id = {weekday}'
        cursor.execute(query)
        query_res = cursor.fetchall()
        logging.info(query_res[0])
        return query_res[0]

    get_data = PythonOperator(
        task_id='get_data',
        provide_context=True,
        python_callable=get_heading,
        op_args=['{{ dag_run.logical_date.weekday() + 1 }}'],
    )

    end = DummyOperator(task_id="end")

    start >> get_data >> end


