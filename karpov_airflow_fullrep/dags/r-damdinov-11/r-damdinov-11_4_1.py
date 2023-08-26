from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
import logging

from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import ShortCircuitOperator


DEFAULT_ARGS = {
    'start_date': datetime(year=2022, month=3, day=1), # Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года
    'end_date': datetime(year=2022, month=3, day=14),
    'owner': 'r-damdinov-11',
    'poke_interval': 600
}

with DAG(
    "r-damdinov-11_4_1",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['r-damdinov-11'],
    ) as dag:

    def is_anything_except_sunday(tomorrow_ds):
        exec_day = datetime.strptime(tomorrow_ds, '%Y-%m-%d').weekday()
        return exec_day != 6

    is_anything_except_sunday_operator = ShortCircuitOperator(
        task_id='is_anything_except_sunday',
        python_callable=is_anything_except_sunday,
        op_kwargs={"tomorrow_ds": "{{ tomorrow_ds }}"}
    )

    def load_from_postgres(tomorrow_ds):
        logging.info(f"tomorrow_ds: {tomorrow_ds}")

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        weekday = datetime.strptime(tomorrow_ds, '%Y-%m-%d').isoweekday()

        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')

        query_res = cursor.fetchall()
        logging.info(f"query_res: {query_res}")

    load_from_postgres_operator = PythonOperator(
        task_id='load_from_postgres',
        python_callable=load_from_postgres,
        op_kwargs={"tomorrow_ds": "{{tomorrow_ds}}"}
    )

    is_anything_except_sunday_operator >> load_from_postgres_operator
