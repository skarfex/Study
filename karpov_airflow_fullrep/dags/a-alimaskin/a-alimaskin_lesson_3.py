# -*- coding: utf-8 -*-
"""
Мой Первый ДАГ
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-alimaskin',
    'poke_interval': 300
}

with DAG('a-alimaskin_lesson_3',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-alimaskin']
) as dag:

    dummyOp = DummyOperator(task_id="dummy")

    bashOp = BashOperator(
        task_id='bashOp',
        bash_command='echo BashOperator is working properly!'
    )

    def python_op_func():
        logging.info('PythonOperator is working properly')

    pythonOp = PythonOperator(
        task_id='pythonOp',
        python_callable=python_op_func,
    )

    dummyOp >> bashOp >> pythonOp