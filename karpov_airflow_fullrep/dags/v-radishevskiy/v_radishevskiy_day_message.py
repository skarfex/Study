"""
Тестовый dag, возвращающий заголовок articles в зависимости от дня недели.
Работает с понедельника по субботу

"""
from datetime import datetime
import logging

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'v-radishevskiy',
    'poke_interval': 600
}

with DAG(
        dag_id="v-radishevskiy-day-message",
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        schedule_interval='0 0 * * 1-6', #every day-of-week from Monday through Saturday
        tags=['v-radishevskiy', 'test'],
        catchup=True,

) as dag:

    dummy_start = DummyOperator(task_id='dummy_start')

    def select_heading_by_weekday(logical_date=None, **kwargs):
        weekday = logical_date.weekday() + 1
        logging.info(f"Current weekday={weekday} (logical_date={logical_date})")

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn() # берём из него соединение
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')  # исполняем sql
        one_string = cursor.fetchone()[0]  # если вернулось единственное значение

        kwargs['ti'].xcom_push(key='weekday', value=weekday)
        return one_string

    select_heading = PythonOperator(
        task_id='display_variables',
        python_callable=select_heading_by_weekday,
        provide_context=True
    )

    dummy_start >> select_heading
