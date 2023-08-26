"Задание 4. Читает с GP"

from airflow import DAG
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import logging

DEFAULT_ARGS = {
    'start_date': datetime(2021, 3, 2),
    'end_date': datetime(2025, 3, 15),
    'owner': 'a-marin',
    'poke_interval': 120
}

with DAG(
        dag_id='a-marin_num4',
        schedule_interval='0 0 * * 2-7',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['a-marin']
) as dag:

    start = DummyOperator(task_id='start_dag')


    def day_and_gp_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor('named_cursor_name')
        day = kwargs['execution_date'].weekday() + 1
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day}')
        query_res = cursor.fetchall()


        logging.info('$$$$$$$$$$$$$$$$$$')
        logging.info(f'Day: {day}')
        logging.info(f'Result: {query_res}')
        logging.info('$$$$$$$$$$$$$$$$$$')

    day_and_gp = PythonOperator(
        task_id='day_and_gp',
        python_callable=day_and_gp_func,
        provide_context=True
    )

    finish = DummyOperator(
        task_id='finish_dag'
    )

    start >> day_and_gp >> finish
