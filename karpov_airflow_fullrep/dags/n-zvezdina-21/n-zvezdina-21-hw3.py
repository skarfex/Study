"""
The following DAG displays the date and extract part of data for this date from GreenPlum
"""
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'n-zvezdina-21'
}

with DAG('n-zvezdina-21-hw',
         schedule_interval='0 0 * * 1-6',
         max_active_runs=1,
         default_args=DEFAULT_ARGS,
         tags=['n-zvezdina-21']
         ) as dag:
    dummy = DummyOperator(task_id="dummy")

    echo_date = BashOperator(
        task_id='echo_date',
        bash_command='echo {{ ds }}'
    )

    def print_date_func(**kwargs):
        logging.info(kwargs['ds'])

    print_date = PythonOperator(
        task_id='print_date',
        python_callable=print_date_func,
        provide_context=True
    )

    def extract_func(**kwargs):
        day = datetime.strptime(kwargs['ds'], '%Y-%m-%d').weekday() + 1

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day}')
        res = cursor.fetchone()[0]
        logging.info(f'Result: {res}')

    extract_from_gp = PythonOperator(
        task_id='extract_from_gp',
        python_callable=extract_func,
        provide_context=True
    )

    dummy >> [echo_date, print_date] >> extract_from_gp
