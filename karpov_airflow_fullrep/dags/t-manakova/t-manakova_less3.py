import logging
from textwrap import dedent

from airflow.macros import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.dates import days_ago

from airflow import DAG

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 't-manakova',
    'poke_interval': 600,
    'retries': 2,
    'start_date': datetime(2022, 12, 30),
    'end_date': datetime(2023, 1, 1),
}

info_str = dedent("""
______________________________________________________________
Date: {{ ds }}
DAG: {{ dag }}
Next: {{ next_ds }}
_____________________________________________________________
""")

with DAG(
    dag_id='t-manakova_simple_dag',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['t-manakova']
) as dag:

    wait_until_1pm = TimeDeltaSensor(
        task_id='wait_until_1pm',
        delta=timedelta(seconds = 13 * 60 * 60))

    end = DummyOperator(task_id='end')

    def print_log(print_this):
        logging.info(print_this)
    
    print_templates = PythonOperator(
    task_id='print_templates',
    python_callable=print_log,
    op_args=[info_str])

wait_until_1pm >> print_templates >> end