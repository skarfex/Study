"""
Забирает из таблицы articles значения поля heading по условию: articles.id={день недели}.
Выводит результат работы в логах.
Отрабатывает по датам с 1 марта 2022 по 14 марта 2022, не работает по воскресеньям
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1, 5),
    'end_date': datetime(2022, 3, 14, 5),
    'owner': 'i-ilichev',
    'poke_interval': 600,
    'trigger_rule': 'all_success'
}

with DAG("i-ilichev_lesson_4",
         schedule_interval='0 5 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=3,
         tags=['i-ilichev']
         ) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def sql_result_func(exec_date):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # и именованный (необязательно) курсор

        cursor.execute(
            'SELECT heading FROM articles WHERE id = %s',
            (datetime.strptime(exec_date, '%Y-%m-%d').weekday()+1,)
        )  # исполняем sql

        # cursor.execute('SELECT heading FROM articles WHERE id = 2')

        query_res = cursor.fetchall()  # полный результат
        # one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info(query_res)  # выведем в логи результат запроса
        conn.close

    sql_result = PythonOperator(
        task_id='sql_result',
        python_callable=sql_result_func,
        dag=dag,
        op_kwargs={'exec_date': '{{ds}}'},
    )

    dummy >> echo_ds >> sql_result