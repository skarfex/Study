"""
Доработка тестового дага

Проверяем execution_date, 
если не воскресенье идем в GreenPlum и подствляем день недели в ксловие фильтрации
и смотрим какая это статья
"""



from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pendulum
import logging
from datetime import datetime
from typing import Any


DEFAULT_ARGS = {
    "start_date": pendulum.datetime(2022, 3, 1, tz="utc"),
    "end_time": pendulum.datetime(2022, 3, 14, tz="utc"),
    "owner": "d-milovanov-3",
    "poke_interval": 60
}

with DAG("d-milovanov-3_lesson4",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=["d-milovanov-3", "lesson-4"]
         ) as dag:
    
    def get_the_number_of_the_day_execution(datetime_execution: str) -> int:
        """
        Функция get_the_number_of_the_day_execution() принимает на вход строку даты запуска дага,
        и вычисляет какой это день недели от 1 (понедельник) до 7 (воскресенья).
        Возвращает True если день недели с понедельника по субботу

        Аргументы функции:
        datetime_execution -- str (дата запуска дага)
        """
        logging.info(datetime_execution)
        number_day_of_execution = datetime.strptime(datetime_execution, "%Y-%m-%d").isoweekday()
        return number_day_of_execution <= 6

    day_is_not_sunday = ShortCircuitOperator(
        task_id="not_sunday",
        python_callable=get_the_number_of_the_day_execution,
        op_kwargs={"datetime_execution": "{{ ds }}"}
    )


    def get_string_of_database(datetime_execution: str) -> Any:
        """
        Функция get_string_of_database() принимает на вход строку даты запуска дага,
        преобразует ее в число дня недели, создает подключение к GreenPlum, 
        отсортировывает в SQL запросе по id статьи по дате написания

        Аргументы функции:
        datetime_execution -- str (дата запуска дага)
        """
        number_day_of_execution = datetime.strptime(datetime_execution, "%Y-%m-%d").isoweekday() 
        pg_hook = PostgresHook(postgres_conn_id="conn_greenplum")
        conn = pg_hook.get_conn()
        cursor = conn.cursor("get_heading_from_articles")
        cursor.execute(f"SELECT heading FROM articles WHERE id = {number_day_of_execution}")
        query_res = cursor.fetchall()
        logging.info('--------------')
        logging.info(f'{number_day_of_execution} of {datetime_execution}')
        logging.info(query_res)
        logging.info('--------------')

    get_string = PythonOperator(
        task_id="get_heading",
        python_callable=get_string_of_database,
        op_kwargs={"datetime_execution": "{{ ds }}"}
    )

day_is_not_sunday >> get_string