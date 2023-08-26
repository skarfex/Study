"""
Урок 3 даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import date

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v_emeljanov',
    'poke_interval': 600
}

with DAG("v_emeljanov_lesson3",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['v-emeljanov']
         ) as dag:

    dummy = DummyOperator(task_id="dummy")  # dummy operator

    date_bash = BashOperator(
        task_id='date_bash',
        bash_command='echo {{ ds }}',
        dag=dag
    )   # bash operator

    def date_func():
        exec_dt = date.today().strftime('%Y-%m-%d')
        logging.info(exec_dt)

    date_py = PythonOperator(
        task_id='date_py',
        python_callable=date_func,
        dag=dag
    )   # python operator

    dummy >> [date_bash, date_py]
