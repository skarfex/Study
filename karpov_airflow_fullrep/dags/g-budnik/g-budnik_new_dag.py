"""
Даг получает информацию из greenplum 
из таблицы articles значение поля heading из строки с id, равным дню недели ds

Не работает по воскресеньям

Работает с 01.04.2014 по 14.03.2022
"""
from datetime import datetime

import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy_operator import DummyOperator


DEFAULT_ARGS = {
    "start_date": datetime(2022, 3, 1),
    "end_date": datetime(2022, 3, 14),
    "owner": "gleb_dag",
    "poke_interval": 300,
}

with DAG(
    "gleb_new_dag",
    schedule_interval="0 5 * * 1-6",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["gleb_new_dag"],
    catchup=True,
) as dag:
    dummy = DummyOperator(task_id="dummy")

    def load_data(**context):
        pg_hook = PostgresHook(postgres_conn_id="conn_greenplum")
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        logging.info(f'execution_date {context["execution_date"]}')
        logging.info(f'ds {context["ds"]}')
        logging.info(f"datetime {datetime.now()}")

        weekday = datetime.strptime(context["ds"], "%Y-%m-%d").weekday() + 1
        cursor.execute(f"SELECT heading FROM articles WHERE id = {weekday}")
        one_string = cursor.fetchone()[0]
        logging.info(one_string)

    print_load_data = PythonOperator(
        task_id="load_data", python_callable=load_data, provide_context=True
    )

    dummy >> print_load_data
