"""
Lesson_4
?
"""

from airflow import DAG
import logging
import pendulum
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'owner': 'n-kislitsyn',
    'start_date': pendulum.datetime(2022, 3, 1),
    'end_date': pendulum.datetime(2022, 3, 14)
}


with DAG("n-kislitsyn-dag-4",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['n-kislitsyn']
         ) as dag:

    def db_worker(week_day, **kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum') # инициализируем хук
        conn = pg_hook.get_conn() # берём из него соединение
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {week_day}') # исполняем sql
        one_string = cursor.fetchone()[0] # если вернулось единственное значение
        logging.info(f'heading: {one_string}')

    lesson = PythonOperator(
        task_id='lesson-4',
        python_callable=db_worker,
        op_args=['{{ logical_date.weekday() + 1 }}']
    )

    lesson