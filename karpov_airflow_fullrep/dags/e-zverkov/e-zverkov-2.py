"""
Автоматизация ETl-процессов
4 Урок
Домашняя работа
"""

import logging
from airflow import DAG
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash_operator import BashOperator


DEFAULT_ARGS = {
    'owner': 'e-zverkov',
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'retries': 3,
    'poke_interval': 600
}

with DAG('e-zverkov_dag_2',
    schedule_interval='5 4 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=3,
    tags=['e-zverkov']
    ) as dag:

    def sql_result(execution_dt):
        execution_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday() + 1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {execution_day}')
        query_res = cursor.fetchall()
        print(datetime.strptime(execution_dt, '%Y-%m-%d'), query_res)
        return query_res

    my_result = PythonOperator(
        task_id='my_result',
        python_callable=sql_result,
        op_kwargs={'execution_dt': '{{ ds }}'}
    )

    my_result
