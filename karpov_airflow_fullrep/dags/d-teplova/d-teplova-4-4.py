"""
Даг для модуля 4, урока 4
"""
from airflow import DAG
from datetime import datetime
import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'owner': 'd-teplova',
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'poke_interval': 600
}

with DAG(
        "d-teplova-4-4",
        schedule_interval='0 3 * * 1-6',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['d-teplova']
) as dag:
    def extract_postgres(load_date):
        execution_dt = datetime.strptime(load_date, '%Y-%m-%d').isoweekday()
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # и курсор
        cursor.execute(f"SELECT heading from articles where id = {execution_dt}")  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        logging.info('--------------')
        logging.info(f'{load_date}')
        logging.info(query_res)
        logging.info('--------------')

    get_articles = PythonOperator(
        task_id='get_articles',
        python_callable=extract_postgres,
        op_kwargs={'load_date': '{{ ds }}'}
        )

    get_articles

