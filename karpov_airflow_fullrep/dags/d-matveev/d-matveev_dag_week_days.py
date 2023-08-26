"""
Это простейший даг.
Он состоит из ShortCircuitOperator (таски запускаются в будни дни),
баш-оператора (выводит execution_date),
питон-оператора (выводит строку в логи),
dummy-оператора
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime
import time

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'd-matveev',
    'poke_interval': 600
}

with DAG("week_day_d_matveev",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['d-matveev_weekday']
) as dag:

       
    def week_days_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return exec_day not in [5, 6]

    week_days = ShortCircuitOperator(
        task_id='week_days',
        python_callable=week_days_func,
        op_kwargs={'execution_dt': '{{ ds }}'}
    )
           
    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ts }}',
        dag=dag
    )

    def working_day_func():
        logging.info("Working day!")

    working_day = PythonOperator(
        task_id='working_day',
        python_callable=working_day_func,
        dag=dag
    )

    week_days >> dummy >> [echo_ds, working_day]
    
week_days.doc_md = """ShortCircuitOperator (таски запускаются в будни дни)"""
echo_ds.doc_md = """Пишет в лог execution_date (2021-01-01T00:00:00+00:00)"""
working_day.doc_md = """working_day'"""
dummy.doc_md = """Dummy task"""