"""
Дз airflow 4 урок
"""

from airflow import DAG
import logging

from datetime import datetime

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'owner': 'k-nameverchenko-19',
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'poke_interval': 600
}

with DAG(
    dag_id="kv_test_4_1",
    schedule_interval='0 3 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['k-nameverchenko-19']
) as dag:
    def get_from_greenplum(**kwargs):
        logging.info(kwargs['ds'])
        weekday = datetime.strptime(kwargs['ds'], '%Y-%m-%d').weekday() + 1
        logging.info(str(weekday))
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        logging.info(query_res[0])

    get_from_greenplum = PythonOperator(
        task_id='get_from_greenplum',
        python_callable=get_from_greenplum,
        provide_context=True
    )