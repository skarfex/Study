############################################################
# Author    Timofey Melnikov                               #
# Email     t.melnikov@okko.tv                         #
############################################################

# System
from datetime import datetime

# Airflow
from airflow import DAG
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def print_conf():
    logging.info('All confs {{ conf }}')


DEFAULT_ARGS = {
    'owner': 't-melnikov-15',
    'start_date': datetime.now()
}

with DAG("t-melnikov_first_dag",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         tags=['t-melnikov-15', 'first_dag']
         ) as dag:
    # dummy
    dummy = DummyOperator(task_id="dummy")
    # bash
    echo_today = BashOperator(
        task_id='print_hello',
        bash_command='echo Today is {{ execution_date }}'
    )
    # python
    hello_world = PythonOperator(
        task_id='print_conf',
        python_callable=print_conf
    )
    # end bash
    bye = BashOperator(
        task_id='print_bye',
        bash_command='echo bye'
    )
    dummy >> [hello_world, echo_today] >> bye
