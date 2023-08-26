"""
DAG Валиева Карима
Модуль 2, урок 3
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from datetime import timedelta, datetime
from random import randint

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.time_delta_sensor import TimeDeltaSensor

DEFAULT_ARGS = {
    'start_date': datetime(2022, 12, 1),
    'owner': 'k-valiev',
    'email': ['rabacc2022@yandex.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'sla': timedelta(hours=3),
    'poke_interval': 600}

with DAG(
        dag_id='k-valiev_module2_step3_dag',
        schedule_interval='0 10 * * *',  # cron interval
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['k-valiev']
) as dag:
    start = DummyOperator(task_id='start')

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',  # execution date
        dag=dag
    )

    wait_1_min = TimeDeltaSensor(
        task_id='wait_1_min',
        delta=timedelta(minutes=1),
        dag=dag
    )


    def hello_world_func():
        logging.info('Hello world!')


    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )


    def see_you_func():
        day = str(randint(1, 7))  # random day
        logging.info(f'The day we will see is {day}')


    see_you = PythonOperator(
        task_id='see_you',
        python_callable=see_you_func,
        dag=dag
    )


    def end_func():
        logging.info("Successfully")


    end = PythonOperator(
        task_id='end',
        python_callable=end_func,
        dag=dag
    )
    # dag
    start >> echo_ds >> wait_1_min >> hello_world >> see_you >> end
