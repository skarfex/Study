import logging
import random

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator, task
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.dates import timedelta, days_ago
from airflow.utils.timezone import datetime

DEFAULT_ARGS = {
    'owner': 'verkhovtsev',
    # 'queue': '--',
    # 'pool': 'user_pool',
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # 'depends_on_past': False,
    'wait_for_downstream': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'priority_weight': 10,
    # 'start_date': datetime(2021, 1, 1),
    # 'end_date': datetime(2025, 1, 1),
    # 'sla': timedelta(hours=2),
    'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    'trigger_rule':  'all_success'
}

def simple_selector():
    return random.choice(["pyop", "bashop"])


with DAG(
    dag_id="simple-dag",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(12),
    max_active_runs=30,
    tags=["verkhovtsev", "first-dag"],
    schedule_interval="@daily",
) as vdag:


    starter = TimeDeltaSensor(
        task_id="time_delta",
        delta=timedelta(seconds=6 * 60 * 60)
    )

    random_switcher = BranchPythonOperator(
        task_id="select_py_or_bash",
        python_callable=simple_selector
    )

    @task(task_id="pyop")
    def print_nice_output(output: str):
        logging.info(f"Nice output: {output}")

    pyop = print_nice_output("PYTHON OP")

    bashop = BashOperator(
        task_id="bashop",
        bash_command='echo "BASH COMMA triggered"'
    )

    @task(task_id="ender")
    def ender_func():
        logging.info("All over!")

    ender = ender_func()
    starter >> random_switcher >> [pyop, bashop] >> ender



