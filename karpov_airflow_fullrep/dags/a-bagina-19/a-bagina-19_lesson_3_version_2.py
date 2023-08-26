"""
3 урок
DAG с выводом даты
4 урок
DAG по определенному периоду и условиям
"""
from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime

# работа с таймзонами
import pendulum


import logging

DEFAULT_ARGS = {
    "start_date": pendulum.datetime(2022, 3, 1, tz="UTC"),
    "end_date": pendulum.datetime(2022, 3, 14, tz="UTC"),
    # schedule_interval='0 0 * * 1-6', # расписание без воскресенья
    "owner": "a-bagina-19",
    "poke_interval": 600,
}


with DAG(
    "a-bagina-19_lesson_3_version_2",
    schedule_interval="@daily",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["a-bagina-19"],
) as dag:

    def get_today():
        logging.info("Current date: {{ ds }}")

    today = PythonOperator(
        task_id="today",
        python_callable=get_today,
    )

    def get_day_of_week(execution_date):
        logging.info(execution_date)
        if datetime.strptime(execution_date, "%Y-%m-%d").weekday() in [6]:
            return "sunday"
        return "weekday"

    choose_day = BranchPythonOperator(
        task_id="choose_day",
        python_callable=get_day_of_week,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    sunday = DummyOperator(task_id="sunday")

    def get_head_line(execution_date):
        current_week_day_id = (
            datetime.strptime(execution_date, "%Y-%m-%d").weekday() + 1
        )
        logging.info(f"Current week day: {current_week_day_id}")

        pg_hook = PostgresHook(postgres_conn_id="conn_greenplum")  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor(
            "named_cursor_name"
        )  # и именованный (необязательно) курсор
        cursor.execute(
            f"SELECT heading FROM articles WHERE id = {current_week_day_id}"
        )  # исполняем sql
        one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info(f"Result: {one_string}")

    weekday = PythonOperator(
        task_id="weekday",
        python_callable=get_head_line,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    today >> choose_day >> [weekday, sunday]
