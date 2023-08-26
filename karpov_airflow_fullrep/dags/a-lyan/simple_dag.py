"""Init simple dag."""
import datetime as dt
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {"owner": "a-lyan", "poke_interval": 600}

dag = DAG(
    "a-lyan-lesson-2",
    start_date=dt.datetime(2023, 4, 1),
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["lyan", "lesson-2"],
)

dummy = DummyOperator(task_id="dummy", dag=dag)


def print_date_func(**kwargs):
    logging.info("--------------------------------")
    logging.info("Date: " + kwargs["ds"])
    logging.info("--------------------------------")


print_date = PythonOperator(
    task_id="print_date", python_callable=print_date_func, provide_context=True
)

echo_ds = BashOperator(task_id="echo_ds", bash_command="echo {{ ds }}", dag=dag)

dummy >> print_date >> echo_ds
