"""
Даг домашняя работа 4го урока
"""
from airflow import DAG
import logging
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'd-miheev-16',
    'poke_interval': 600
}

with DAG("ds_test_miheev_hw04",
         schedule_interval='1 3 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['d-miheev-16', 'hw04']
         ) as dag:

    def start_log():
        logging.info("DAG STARTED")


    start_operator = PythonOperator(
        task_id='start_operator',
        python_callable=start_log
    )

    def end_log():
        logging.info("DAG END")

    end_operator = PythonOperator(
        task_id='end_operator',
        python_callable=end_log
    )


    def select_func(**kwargs):
        this_date = kwargs['ds']
        day_of_week = datetime.strptime(this_date, '%Y-%m-%d').weekday() + 1

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук

        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')
        one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info(f'RESULT = {one_string}')


    select_operator = PythonOperator(
        task_id='select_operator',
        python_callable=select_func,
        provide_context=True
    )

    start_operator >> select_operator >> end_operator
