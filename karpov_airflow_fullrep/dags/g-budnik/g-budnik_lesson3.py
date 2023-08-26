"""
Тестовый даг
"""
import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {"start_date": days_ago(2), "owner": "gleb_dag", "poke_interval": 600}

with DAG(
    "gleb_simple_dag",
    schedule_interval="@daily",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["gleb_dag"],
) as dag:
    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(task_id="echo_ds", bash_command="echo {{ ds }}", dag=dag)

    def python_date_func():
        date_to_day = datetime.date.today().strftime("%d-%m-%Y")
        logging.info(date_to_day)
        print(date_to_day)

    python_date = PythonOperator(
        task_id="python_date", python_callable=python_date_func
    )

    dummy >> [echo_ds, python_date]
