"""
Тестовый даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def work_hours():
    tm = datetime.now().strftime('%H:%M')
    work_start_tm = datetime(12, 1, 3, 9).strftime('%H:%M')
    work_end_tm = datetime(12,1,3,18).strftime('%H:%M')
    wkd = datetime.now().weekday()
    if tm > work_start_tm and tm < work_end_tm and wkd < 5:
        return "ВСТАВАЙ РАБОТАТЬ ПОРА!!!"
    elif wkd < 5 and (tm < work_start_tm or tm > work_end_tm):
        return "РАБОЧЕЕ ВРЕМЯ ЗАКОНЧИЛОСЬ ОТДЫХАЙ!!!"
    elif wkd > 5:
        return "СЕГОДНЯ ВЫХОДНОЙ ОТДЫХАЙЭ"

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'd-evteev',
    'poke_interval': 600
}

with DAG("d-evteev_task1",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['d-evteev'],
    user_defined_macros = {'my_cust_func': work_hours()}
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ macros.ds_add(ds, +10) }}',
        dag=dag
    )

    work_time = BashOperator(
        task_id='work_time',
        bash_command='echo {{ my_cust_func }}',
        dag=dag
    )

    def hello_world_func():
        a = 5 + 5
        logging.info("Hello World! ", a)

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )

    def bye_world_func():
        a = 50 + 50
        logging.info("Bye bye World!! ",a)

    bye_world = PythonOperator(
        task_id='bye_world',
        python_callable=bye_world_func,
        dag=dag
    )

    bash_bye_world = BashOperator(
        task_id='bash_bye_world',
        bash_command='ls',
        dag=dag
    )

    dummy >> [echo_ds, hello_world] >> work_time >> [bash_bye_world, bye_world]