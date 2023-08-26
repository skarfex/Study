"""
Lesson 4 dag
"""
from airflow import DAG
from datetime import datetime
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'ed-kuzhamuratov',
    'poke_interval': 600
}

with DAG(
    "ed-kuzhamuratov_lesson4",
    schedule_interval='0 6 * * 1-6',
    catchup=True,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['ed-kuzhamuratov']
) as dag:

    start = DummyOperator(task_id="start")

    def read_heading_and_print(exec_dt):
        weekday = datetime.strptime(exec_dt, '%Y-%m-%d').weekday()  # getting the
        logging.info(f'Execution date: {exec_dt}; Weekday: {weekday+1}')

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # initializing the hook
        conn = pg_hook.get_conn()  # get the connection
        cursor = conn.cursor()  # creating a cursor
        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday+1}')  # run the parameterized sql
        one_string = cursor.fetchone()[0]  # get the first result

        logging.info(one_string)

    print_heading = PythonOperator(
        task_id='print_heading',
        python_callable=read_heading_and_print,
        op_kwargs={'exec_dt': '{{ ds }}'},
        dag=dag
    )

    end = DummyOperator(task_id="end")

    start >> print_heading >> end