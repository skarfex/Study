"""
Задание
1. Работать с понедельника по субботу, но не по воскресеньям
2. Ходить GreenPlum. Вариант решения — PythonOperator с PostgresHook внутри
3. Использует соединение 'conn_greenplum'
4. Забирает из таблицы articles значение поля heading из строки с id, равным дню недели ds (понедельник=1, вторник=2, ...)
5. Выводит результат работы в логах
6. Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года
"""

import pendulum
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.weekday import WeekDay
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.weekday import BranchDayOfWeekOperator

from click import echo

DEFAULT_ARGUMENTS = {
    'owner': 'r-atamov',
    'poke_interval': 30,
}

with DAG(
    'ragim_work_daily_simple',
    default_args=DEFAULT_ARGUMENTS,
    description='My simple DAG',
    schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 3, 1),
    end_date=pendulum.datetime(2022, 3, 14),
    max_active_runs=1,
    catchup=True,
    tags=['ragim_daily'],
) as dag:

    start = DummyOperator(task_id="start")

    branch_week_day = BranchDayOfWeekOperator(
        task_id='branch_week_day',
        use_task_execution_day=True,
        week_day={WeekDay.MONDAY, WeekDay.TUESDAY, WeekDay.WEDNESDAY, WeekDay.THURSDAY,
                  WeekDay.FRIDAY, WeekDay.SATURDAY},
        follow_task_ids_if_true="get_heading_articles",
        follow_task_ids_if_false="end")

    def get_heading_articles_func(ds):
        # Получаем запланированный день недели
        # execution_date = kwargs['execution_date']
        target_weekday = pendulum.from_format(ds, 'YYYY-MM-DD').day_of_week
        # инициализируем хук
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')

        with pg_hook.get_conn() as conn:
            cursor = conn.cursor()  # и курсор
            # исполняем sql
            cursor.execute(
                f"SELECT heading FROM articles WHERE id = {target_weekday}")
            # Выводим результат
            for row in cursor:
                logging.info(row)

    get_heading_articles = PythonOperator(
        task_id='get_heading_articles',
        # provide_context=True,
        python_callable=get_heading_articles_func,
    )

    end = DummyOperator(task_id="end")

    start >> branch_week_day >> [get_heading_articles, end]
