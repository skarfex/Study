"""
Это простейший даг.
Он состоит из сенсора (ждёт 6am),
баш-оператора (выводит execution_date),
двух питон-операторов (выводят по строке в логи)
"""

import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {"start_date": days_ago(12), "owner": "a-lyan", "poke_interval": 600}

dag = DAG(
    "a_simple_dag",
    schedule_interval="@daily",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["lyan"],
)

dummy = DummyOperator(task_id="dummy", dag=dag)

echo_ds = BashOperator(task_id="echo_ds", bash_command="echo {{ ds }}", dag=dag)


def first_func():
    logging.info("First log")


first_task = PythonOperator(task_id="first_task", python_callable=first_func, dag=dag)


def second_func():
    logging.info("Second log")


second_task = PythonOperator(
    task_id="second_task", python_callable=second_func, dag=dag
)

dummy >> echo_ds >> [first_task, second_task]
