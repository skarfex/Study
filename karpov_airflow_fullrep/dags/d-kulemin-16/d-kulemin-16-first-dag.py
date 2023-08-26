"""
Первый простой даг.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import datetime as dt
import random

from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(12),
    'owner': 'd-kulemin-16',
    'poke_interval': 600
}

with DAG(
    "d-kulemin-16-first",
    schedule_interval='0 0 * * *',
    default_args=DEFAULT_ARGS,
    max_active_runs=8,
    tags=['d-kulemin-16']
) as dag:
    wait_until_7am = TimeDeltaSensor(
        task_id='wait_until_7am',
        delta=dt.timedelta(seconds=7*60*60),
    )

    start = DummyOperator(task_id='start')

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}'
    )

    def select_random_task():
        return random.choice(['dummy1', 'dummy2', 'dummy3'])

    select_random = BranchPythonOperator(
        task_id='select_random',
        python_callable=select_random_task,
    )

    dummy1 = DummyOperator(task_id='dummy1')
    dummy2 = DummyOperator(task_id='dummy2')
    dummy3 = DummyOperator(task_id='dummy3')

    def select_task_by_day(**kwargs):
        execution_dt = kwargs['execution_dt']
        exec_day = dt.datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return 'weekend' if exec_day in [5, 6] else 'weekday'

    weekday_or_weekend = BranchPythonOperator(
        task_id='weekday_or_weekend',
        python_callable=select_task_by_day,
        op_kwargs={'execution_dt': '{{ ds }}'}
    )

    def weekday_announce(owner):
        logging.info(f"{owner}, it's time to work!")

    weekday = PythonOperator(
        task_id='weekday',
        python_callable=weekday_announce,
        op_args = ['{{ task.owner }}']
    )

    def weekend_announce(**kwargs):
        logging.info(f"{kwargs['owner']}, it's party time!")

    weekend = PythonOperator(
        task_id='weekend',
        python_callable=weekend_announce,
        op_kwargs = {'owner': '{{ task.owner }}'}
    )

    wait_until_7am >> start >> echo_ds >> select_random >> [dummy1, dummy2, dummy3]
    echo_ds >> weekday_or_weekend >> [weekday, weekend]
