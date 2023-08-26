"""
Тестовый даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import date
import datetime
from datetime import timedelta
import logging

from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v-kosinov',
    'poke_interval': 600
}

with DAG('kosinov_less_1',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['v-kosinov']) as dag:


    start_at_00 = TimeDeltaSensor(
        task_id='start_at_00',
        delta=timedelta(seconds=0)
    )

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}'
    )

    def calc_days():
        a = str(datetime.datetime(2023, 1, 1) - datetime.datetime.now())
        b = 'days_before_xmas:' + a
        logging.info(b)

    days_before_xmas = PythonOperator(
        task_id='days_before_xmas',
        python_callable=calc_days
    )

    start_at_00 >> echo_ds >> days_before_xmas