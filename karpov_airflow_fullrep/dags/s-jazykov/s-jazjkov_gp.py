"""
Задание 3.4
"""

import logging
import pendulum
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1),
    'end_date': pendulum.datetime(2022, 3, 14),
    'owner': 's-jazykov',
    'poke_interval': 600
}

def pg_hook(**kwargs):
    date = kwargs['ds']
    dow = pendulum.parse(date).day_of_week
    request = f"SELECT heading FROM articles WHERE id = { dow }"
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(request)
    result = cursor.fetchall()
    logging.info(f'Result of query is {result}')

with DAG("s-jazykov_gp",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['s-jazykov']
) as dag:

    get_data = PythonOperator(
        task_id='get_data',
        python_callable=pg_hook,
        provide_context=True
    )

    start = DummyOperator(task_id='start')

    start >> get_data