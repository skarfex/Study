"""
Работать с понедельника по субботу, но не по воскресеньям
Забирать из таблицы articles значение поля heading из строки с id, равным дню недели ds (понедельник=1, вторник=2, ...)
Выводить результат работы в любом виде: в логах либо в XCom'е
Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года
"""

from airflow import DAG
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': '2022-03-01',
    'end_date': '2022-03-14',
    'owner': 'k-smirnov'
}

with DAG("k-smirnov_test", schedule_interval='0 0 * * 1-6', default_args=DEFAULT_ARGS, max_active_runs=1,
         tags=['k-smirnov'],
         ) as dag:
    def get_rows_gp(day_week):
        pg_hook = PostgresHook('conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM public.articles WHERE id = {day_week}')
        logging.info(f'День недели запуска DAG {day_week}, Получен результат: {cursor.fetchone()[0]}')


    rows_gp = PythonOperator(
        task_id='get_rows',
        python_callable=get_rows_gp,
        op_args=['{{ dag_run.logical_date.strftime("%w") }}']
    )

    rows_gp
