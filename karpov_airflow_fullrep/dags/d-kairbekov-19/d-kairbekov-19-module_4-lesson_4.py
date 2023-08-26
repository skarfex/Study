"""
Module 4 Lesson 4
"""

# Logging
import logging

# DateTime
import datetime as dt

# AirFlow
from airflow import DAG

# Hooks
from airflow.hooks.postgres_hook import PostgresHook

# Operators
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': dt.datetime(2022, 3, 1),
    'end_date': dt.datetime(2022, 3, 15),
    "owner": "d-kairbekov-19",
    "poke_interval": 60,
}

with DAG(
    "d-kairbekov-19-module_4-lesson_4",
    schedule_interval="0 3 * * 1-6",  # Сдвиг времени на 3 часа для запуска по времени MSK
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["d-kairbekov-19"],
) as dag:

    # Funcs #

    def get_weekday_number(ds, ti):
        logging.info(f"Target date is: {ds}")
        ti.xcom_push(key="number", value=dt.datetime.fromisoformat(ds).weekday())

    def get_articles_heading(ti):
        id = ti.xcom_pull(key="number", task_ids=["weekday_number"])[0] + 1
        logging.info(f"ID is: {id}")
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {id}')  # исполняем sql
        ti.xcom_push(key="heading", value=cursor.fetchone()[0])

    # Tasks #

    dummy = DummyOperator(task_id="dummy")

    weekday_number = PythonOperator(
        task_id="weekday_number",
        python_callable=get_weekday_number,
        dag=dag,
    )
    articles_heading = PythonOperator(
        task_id="articles_heading",
        python_callable=get_articles_heading,
        dag=dag,
    )

    # DAG #

    dummy >> weekday_number >> articles_heading
