"""
Даг для модуля 4, Урока 3
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor

DEFAULT_ARGS={
    'owner': 'd-teplova',
    'start_date': days_ago(2),
    'end_date': datetime(2024, 1, 1),
    'poke_interval': 600
}

with DAG(
    dag_id='d_teplova_module_4_3',
    schedule_interval='@weekly',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags='d-teplova'
) as dag:

    dummy = DummyOperator(
        task_id='dummy',
        trigger_rule='all_success',
        dag=dag
    )

    wait_until_8pm=TimeDeltaSensor(
        task_id='wait_until_8pm',
        delta=timedelta(seconds='0 20 * * 3')
    )

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ds}}',
        dag=dag
    )

dummy >> wait_until_8pm >> echo_ds