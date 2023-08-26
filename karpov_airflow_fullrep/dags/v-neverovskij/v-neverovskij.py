"""
Даг на пять тасков.
"""
from time import sleep
from datetime import datetime
import logging

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1, tz="UTC"),
    'owner': 'v-neverovskij',
}


def task2():
    sleep(1)
    logging.info("task2 is doing job.")


def task3():
    sleep(2)
    logging.info("task1 and task2 is done. Task3 is doing job.")


def get_heading(ds):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    weekday = datetime.strptime(ds, '%Y-%m-%d').weekday()+1
    cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')
    logging.info(f'Week day: {weekday}. Heading: {cursor.fetchone()[0]}')


with DAG("v-neverovskij",
         schedule_interval='0 12 * * Mon-Sat',
         default_args=DEFAULT_ARGS,
         tags=['v-neverovskij'],
         catchup=True
         ) as dag:

    dummy = DummyOperator(task_id="dummy")

    task1 = BashOperator(
        task_id='task1',
        bash_command='echo task1 is doing job.',
    )

    task2 = PythonOperator(
        task_id='task2',
        python_callable=task2,
    )

    task3 = PythonOperator(
        task_id='task3',
        python_callable=task3,
        wait_for_downstream=True
    )

    task4 = PythonOperator(
        task_id='get_heading',
        python_callable=get_heading
    )

    dummy >> [task1, task2] >> task3 >> task4
