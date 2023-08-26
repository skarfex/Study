"""
Lesson 4
"""
from airflow import DAG
from datetime import datetime
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'f-korr',
    'poke_interval': 60
}

with DAG('f-korr_lesson4',
    schedule_interval='0 12 * 3 MON-SAT',
    catchup=True,
    default_args=DEFAULT_ARGS,
    tags=['f-korr', 'ht6o5KnE']
) as dag:

    start = DummyOperator(task_id='start', dag=dag)

    def get_heading_func(**context):
        execution_date = context['execution_date']
        weekday = execution_date.weekday() + 1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')
        query_res = cursor.fetchall()
        context['ti'].xcom_push(value=query_res, key='article_head')
        logging.info(f"{query_res}")

    pull_heading = PythonOperator(
        task_id='pull_heading',
        python_callable=get_heading_func,
        provide_context=True
    )

    end = DummyOperator(task_id='end', dag=dag)

    start >> pull_heading >> end