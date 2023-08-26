# Data Engineer 20
# Module 4
# Task 3


from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import datetime
import logging

DEFAULT_ARGS = {
    "start_date": days_ago(1),
    "owner": "aleksey-ivanov",
    "poke_interval": 600,
}

with DAG(
    "aleksey-ivanov_de20_m4_t3",
    schedule_interval="@daily",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["aleksey-ivanov"],
) as dag:
    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(task_id="echo_ds", bash_command="echo {{ ds }}", dag=dag)

    def print_date_func():
        logging.info(str(datetime.datetime.today()))

    print_date = PythonOperator(
        task_id="print_date", python_callable=print_date_func, dag=dag
    )

    dummy >> [echo_ds, print_date]
