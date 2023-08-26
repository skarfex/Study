"""
Lesson 3
"""
import random
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


def print_hello_world():
    print("Hello world! В этот раз нам повезло!")
    print("^ Printed by PythonOperatorTask")


def random_choice():
    return random.randint(0, 1) % 2


DEFAULT_ARGS = {
    "start_date": days_ago(3),
    "owner": "a-spiridonov-19",
    "email": ["spiridonovandrei@hotmail.com"],
    "email_on_failure": True,
    "retries": 2,
    "sla": timedelta(hours=1),
}

with DAG(
    dag_id="a-spiridonov-19_lesson_3",
    schedule_interval="@once",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    # catchup=False
) as dag:

    echo_ds = BashOperator(
        task_id="echo_ds",
        bash_command="echo {{ ds }}",
    )

    random_choice = ShortCircuitOperator(
        task_id="random_choice", python_callable=random_choice
    )

    hello_world_python = PythonOperator(
        task_id="hello_world_python",
        python_callable=print_hello_world,
    )

    echo_ds >> random_choice >> hello_world_python
