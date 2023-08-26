"""
Даг для чтения данных из Greenplum
"""
from airflow import DAG
from datetime import datetime
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022,3,1),
    'end_date': datetime(2022,3,14),
    'poke_interval': 60,
    'owner': 'v-maslov'
}

with DAG("v_maslov_gp",
         schedule_interval='0 0 * * 2-7',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['v-maslov']
         ) as dag:

    def extract_data_from_gp_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        day_of_week = kwargs['execution_date'].weekday() + 1
        logging.info(f'Current day of week number is {day_of_week}')
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')
        kwargs['ti'].xcom_push(value=cursor.fetchone()[0], key='gp_data')
        conn.commit()

    def print_data_to_log_func(**kwargs):
        logging.info(kwargs['ti'].xcom_pull(task_ids='extract_data_from_gp', key='gp_data'))


    extract_data_from_gp = PythonOperator(
        task_id='extract_data_from_gp',
        python_callable=extract_data_from_gp_func,
        provide_context=True
    )

    print_data_to_log = PythonOperator(
        task_id='print_data_to_log',
        python_callable=print_data_to_log_func,
        provide_context=True
    )

    extract_data_from_gp >> print_data_to_log