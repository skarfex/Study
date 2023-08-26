"Задание 3. Простой даг и нескольких тасков"
import random

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.edgemodifier import Label
import logging

DEFAULT_ARGS = {
    'start_date': days_ago(5),
    'owner': 'a-marin',
    'poke_interval': 120
}

# создаем даг
with DAG(
        dag_id='a-marin_exercise3',
        schedule_interval='@hourly',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['a-marin']
) as dag:
    wait_12am = TimeDeltaSensor(
        task_id='wait_12am',
        delta=timedelta(seconds=12 * 60 * 60))

    start = DummyOperator(task_id='start_dag')


    def date_today_func():
        logging.info('Today is:')


    today = PythonOperator(
        task_id='today',
        python_callable=date_today_func)

    get_date = BashOperator(
        task_id='get_date',
        bash_command='echo {{ ds }}')


    def date_next_day_func():
        logging.info('Next day is:')


    next_day = PythonOperator(
        task_id='next_day',
        python_callable=date_next_day_func)

    get_next_date = BashOperator(
        task_id='get_next_date',
        bash_command='echo {{ tomorrow_ds }}')


    def select_day_func():
        return random.choice(['today', 'next_day'])


    select_day = BranchPythonOperator(
        task_id='select_day',
        python_callable=select_day_func)

    finish = DummyOperator(
        task_id='finish_dag',
        trigger_rule='one_success'
    )

    start >> wait_12am >> select_day >> Label("It's today")  >>today >> get_date >> finish
    select_day >> Label("It's tomorrow") >> next_day >> get_next_date >> finish

dag.doc_md = __doc__
start.doc_md = """Ничего не делает"""
wait_12am.doc_md = """Сенсор. Ждем наступления 12am по Гринвичу"""
select_day.doc_md = """Оператор случайного выбора сегодняшнего или завтрашнего дня"""
today.doc_md = """Пишет в лог Today is"""
get_date.doc_md = """Пишет в лог execution_date"""
next_day.doc_md = """Пишет в лог Next day is"""
get_next_date.doc_md = """Пишет в лог следующий за execution_date"""
finish.doc_md = """Ничего не делает"""
