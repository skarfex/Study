"""
DAG для чтения из Greenplum
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
import logging

from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 2),
    'end_date': datetime(2022, 3, 15),
    'owner': 's-mokrushin',
    'poke_interval': 60
}

with DAG("s-mokrushin_gp_import",
    schedule_interval='0 0 * * 2-7',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['s-mokrushin']
) as dag:


    def get_data_from_greenplum_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()
        day_of_week = kwargs['execution_date'].weekday() + 1
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')
        query_result = cursor.fetchall()
        kwargs['ti'].xcom_push(value=query_result, key=f'heading_{day_of_week}')

        # logging
        logging.info('--------------')
        logging.info(f'Actual day of week number is {day_of_week}')
        logging.info(f'Result {query_result}')
        logging.info('--------------')

    get_data_from_gp = PythonOperator(
        task_id='get_data_from_gp',
        python_callable=get_data_from_greenplum_func,
        provide_context=True
    )

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    start >> get_data_from_gp >> end