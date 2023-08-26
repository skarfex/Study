"""
Задание к уроку №4 "Сложные пайплайны, часть 2"
---
Ходим в GreenPlum
Забираем из таблицы articles значение поля heading из строки с id, равным дню недели ds (понедельник=1, вторник=2, ...).
Выводим результат работы в логах.
"""
from airflow import DAG
from datetime import datetime
import logging

from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'v-zotov',
    'poke_interval': 600
}

with DAG("v-zotov_lesson4_v2",
         schedule_interval='0 0 * * 1-6',
         catchup=True,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['v-zotov']
         ) as dag:

    echo_start = BashOperator(
        task_id='echo_start',
        bash_command='echo {{ ts }}\necho day of week: {{ execution_date.isoweekday() + 1}}\necho started',
        dag=dag
    )

    def select_heading_from_articles_func(weekday):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        logging.info(query_res)


    select_heading_from_articles = PythonOperator(
        task_id='select_heading_from_articles',
        python_callable=select_heading_from_articles_func,
        op_args=['{{ execution_date.isoweekday() + 1}}']
    )

    echo_finish = BashOperator(
        task_id='echo_finish',
        bash_command='echo finished',
        dag=dag
    )

    echo_start >> select_heading_from_articles >> echo_finish
