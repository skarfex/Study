"""
Простой даг - вывод дня недели
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(5),
    'owner': 'g-haritonova-10',
    'poke_interval': 600
}

with DAG("g-haritonova-10-first-dag",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['g-haritonova-10']
          ) as dag:

    dummy = DummyOperator(task_id='dummy')

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def week_day(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return exec_day

    log_week_day = PythonOperator(
        task_id='log_week_day',
        python_callable=week_day,
        op_kwargs={'execution_dt': '{{ ds }}'},
        dag=dag
    )

    dummy >> [echo_ds, log_week_day]


