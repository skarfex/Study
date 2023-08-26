"""
выведем строки из гринплама (кроме вс)
"""
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import ShortCircuitOperator
import datetime as dt


DEFAULT_ARGS = {
    'start_date': dt.datetime(2022, 3, 1),
    'end_date': dt.datetime(2022, 3, 14),
    'owner': 'al-vdovenko',
    'poke_interval': 10
}


with DAG("alexvdo_lesson4",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['al-vdovenko']
         ) as dag:

    start = DummyOperator(task_id='start')

    def weekday_is_not_sunday_check(execution_date):
        return execution_date.weekday() != 6

    not_sunday = ShortCircuitOperator(
        task_id='not_sunday',
        python_callable=weekday_is_not_sunday_check)

    def fill_db(execution_date):
        weekday = execution_date.weekday() + 1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        logging.info(query_res)

    logging_info_from_db = PythonOperator(
        task_id='logging_info_from_db',
        python_callable=fill_db,
        dag=dag
    )

    start >> not_sunday >> logging_info_from_db