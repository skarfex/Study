"""
Lesson 3
Simple dag
"""
import logging

from airflow import DAG
from airflow.utils.dates import days_ago

import datetime as dt

from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    "start_date": days_ago(2),
    "owner": "a-kapaev-20"
}

with DAG(
        "a-kapaev-20-module_2-lesson_3",
        schedule_interval="@once",
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=["a-kapaev-20"],
) as dag:
    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )


    def print_current_date():
        current_date = dt.datetime.now()
        formatted_date = current_date.strftime("%Y-%m-%d")
        print(formatted_date)


    print_python = PythonOperator(
        task_id='python_ds',
        python_callable=print_current_date,
        dag=dag
    )

    dummy >> [echo_ds, print_python]
