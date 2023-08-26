# -*- coding: utf-8 -*-
"""
First DAG
"""

from datetime import datetime
import logging
import random

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task


DEFAULT_ARGS = {
    'owner': 'l-husnutdinov'
}

with DAG(
        dag_id='l-husnutdinov-lesson-3',
        default_args=DEFAULT_ARGS,
        schedule_interval='* * * * *',
        start_date=datetime(year=2022, month=12, day=9),
        tags=['lesson-3', 'l-husnutdinov'],
        catchup=False
) as dag:
    emptyOp = DummyOperator(
        task_id='emptyOp'
    )

    bashOp = BashOperator(
        task_id='bashOp',
        bash_command='date'
    )

    @task(task_id="pythonPrintOp")
    def print_args_func(input_string: str):
        logging.info('Passed string variable: ' + input_string)

    pythonPrintOp = print_args_func('TEST')

    @task(task_id="pythonRandomOp")
    def print_random():
        logging.info(random.choice(["++++", "----"]))

    pythonRandomOp = print_random()

    emptyOp >> bashOp >> pythonPrintOp >> pythonRandomOp
