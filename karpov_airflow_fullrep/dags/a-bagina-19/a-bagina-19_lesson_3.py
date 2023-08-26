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
from airflow.operators.datetime import BranchDateTimeOperator
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime

# работа с таймзонами
import pendulum


import logging

DEFAULT_ARGS = {
    "start_date": pendulum.datetime(2022, 3, 1, tz="UTC"),
    "end_date": pendulum.datetime(2022, 4, 1, tz="UTC"),
    "owner": "a-bagina-19",
    "poke_interval": 600,
}


with DAG(
    "a-bagina-19_lesson_3",
    schedule_interval="@daily",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["a-bagina-19"],
) as dag:

    def get_today():
        logging.info("Current date: {{ macros.datetime.now() }}")

    today = PythonOperator(
        task_id="today",
        python_callable=get_today,
    )

    # Можно реализовать в настройках DEFAULT_ARGS start_date и end_date
    # Но попробуем черз  BranchDateTimeOperator between 2022-03-01 and 2022-03-14
    datetime_period = BranchDateTimeOperator(
        task_id="datetime_period",
        use_task_execution_date=True,
        follow_task_ids_if_true=["choose_day"],
        follow_task_ids_if_false=["date_outside_range"],
        target_upper=datetime(2022, 3, 14, 0, 0, 0),
        target_lower=datetime(2022, 2, 28, 0, 0, 0),
    )

    date_outside_range = DummyOperator(task_id="date_outside_range")

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

    today >> datetime_period >> [choose_day, date_outside_range]
    choose_day >> [weekday, sunday]
