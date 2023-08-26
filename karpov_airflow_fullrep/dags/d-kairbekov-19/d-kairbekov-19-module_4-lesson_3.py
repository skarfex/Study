"""
Module 4 Lesson 3
"""

# Logging
import logging

# DateTime
import datetime as dt

# AirFlow
from airflow import DAG
from airflow.utils.dates import days_ago

# Operators
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    "start_date": days_ago(1),
    "owner": "d-kairbekov-19",
    "poke_interval": 60,
}

with DAG(
    "d-kairbekov-19-module_4-lesson_3",
    schedule_interval="@once",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["d-kairbekov-19"],
) as dag:

    # Funcs #

    def log_hello_world():
        logging.info("hello_world")

    def log_current_date():
        logging.info(f"Current date is: {dt.datetime.now()}")

    # Tasks #

    dummy = DummyOperator(task_id="dummy")
    hello_world = PythonOperator(
        task_id="hello_world",
        python_callable=log_hello_world,
        dag=dag,
    )
    current_date = PythonOperator(
        task_id="current_date",
        python_callable=log_current_date,
        dag=dag,
    )
    echo_ds = BashOperator(
        task_id="echo_ds",
        bash_command="echo {{ ds }}",
        dag=dag,
    )

    # DAG #

    dummy >> [hello_world, current_date, echo_ds]
