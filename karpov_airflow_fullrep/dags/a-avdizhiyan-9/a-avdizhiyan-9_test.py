"""
Тестовый даг
"""
import airflow
#import jinja2
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime
from airflow.decorators import task, dag

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator

#import pytest
#from airflow.models import DagBag

#def test_no_import_errors():
  #dag_bag = DagBag()
  #return len(dag_bag.import_errors)

#test_no_import_errors()

DEFAULT_ARGS = {
    'start_date': days_ago(0,0,0,0,0),
    'owner': 'ann-avdizhiian',
    'poke_interval': 600
}

@dag(
    start_date=datetime(2022, 7, 10),
    schedule_interval="35 * * * *",
    default_args=DEFAULT_ARGS
)
def context_check():
    @task
    def test_task_1():
        print("I am task 1")
        return 1

    @task
    def test_task_2(val: int):
        print("I am task 2")

    @task
    def test_task_3():
        print("I am task 3")

    task_1_result = test_task_1()
    task_2 = test_task_2(task_1_result)
    task_3 = test_task_3()

    task_2 >> task_3

dag = context_check()
