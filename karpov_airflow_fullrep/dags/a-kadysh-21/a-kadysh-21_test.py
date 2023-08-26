from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date
DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-kadysh-21',
    'poke_interval': 600
}

with DAG("a-kadysh-21_test",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-kadysh-21']
    ) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def date_func():
        logging.info(date)

    show_date = PythonOperator(
        task_id='show_date',
        python_callable=date_func,
        dag = dag
    )

    dummy >> [echo_ds, show_date]