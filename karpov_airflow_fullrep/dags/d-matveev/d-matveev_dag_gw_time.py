
"""
Это простейший даг.
Он состоит из сенсора (ждёт 6am 6 min 6 sec),
баш-оператора (выводит execution_date),
питон-оператора (выводит строку в логи),
dummy-оператора
"""

from airflow import DAG
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'd-matveev',
    'poke_interval': 600
}

with DAG("ds_test_d_matveev",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['d-matveev_copy']
) as dag:

    
    wait_until_6am_6min_6sec = TimeDeltaSensor(
    task_id='wait_until_6am_6min_6sec',
    delta=timedelta(seconds=6*60*60+6*60+6)
    )

           
    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ts }}'
    )

    def working_day_func():
        logging.info("Working day!")

    working_day = PythonOperator(
        task_id='working_day',
        python_callable=working_day_func
    )

    wait_until_6am_6min_6sec >> dummy >> [echo_ds, working_day]
    
wait_until_6am_6min_6sec.doc_md = """Сенсор. Ждёт наступления 6am 6 min 6 sec по Гринвичу"""
echo_ds.doc_md = """Пишет в лог execution_date (2021-01-01T00:00:00+00:00)"""
working_day.doc_md = """working_day'"""
dummy.doc_md = """Dummy task"""
