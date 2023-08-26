"""
a-dronov-20
Урок 4. Создание дага, который работает в заданный период во все дни, кроме воскресений.
В результате в лог выводится значение поля heading таблицы articles с id, соответствующим дню недели.
"""

from airflow import DAG
from datetime import datetime
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    'owner': 'a-dronov-20',
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14)
}

with DAG("a-dronov-20_lab3",
         schedule_interval='0 12 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-dronov-20']
         ) as dag:

    dummy = DummyOperator(task_id="dummy")

    def print_heading_date(exec_date):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        week_day_num = datetime.strptime(exec_date, '%Y-%m-%d').isoweekday()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {week_day_num}')
        query_res = cursor.fetchall()[0]
        logging.info(f'Heading = {query_res}')
        cursor.close()
        conn.close()

    heading_print = PythonOperator(
        task_id='heading_print',
        python_callable=print_heading_date,
        op_kwargs={'exec_date': '{{ ds }}'},
        dag=dag
    )

    dummy >> heading_print
