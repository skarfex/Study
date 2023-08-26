"""
Задание из модуля 2 урока 4(1)
"""

from airflow import DAG
from datetime import datetime
import logging

from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022,3,1),
    'end_date': datetime(2022,3,14),
    'owner': 'i-ilhman',
    'poke_interval': 600
}

with DAG("i_ilhman_m4l1",
        schedule_interval='0 0 * * 1-6',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['i-ilhman']
         ) as dag:

    query = 'SELECT heading FROM articles WHERE id = ({{ execution_date.weekday() }} + 1);'

    def get_data_gp_func(query):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(query)
        query_res = cursor.fetchall()
        logging.info('query is ' + query)
        logging.info('result is ' + str(query_res))

    get_data_gp = PythonOperator(
        task_id='get_data_gp',
        python_callable=get_data_gp_func,
        op_args=[query]
    )

get_data_gp