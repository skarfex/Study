"""
Тестовый даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022,3,1),
    'end_date': datetime(2022,3,15),
    'owner': 'm-rasskazov-20',
    'poke_interval': 600
}

with DAG("m-rasskazov-20-test-dag",
    schedule_interval='* * * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['m-rasskazov-20-test-dag']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def hello_world_func():
        logging.info("Hello World!")

    def sel_from_gp_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT heading FROM articles WHERE id in ({0})'.format(datetime.today().isoweekday()))
        return cursor.fetchall()

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )

    sel_from_gp = PythonOperator(
        task_id='sel_from_gp',
        python_callable=sel_from_gp_func,
        dag=dag
    )

    dummy >> [echo_ds, hello_world, sel_from_gp]