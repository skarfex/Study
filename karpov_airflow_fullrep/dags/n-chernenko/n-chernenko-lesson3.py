"""
Тестовый даг урок3
n-chernenko
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'n-chernenko',
    'poke_interval': 600
}

with DAG("n-chernenko_lesson3",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['n-chernenko_lesson3']
         ) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    sleep_minute = BashOperator(
        task_id='sleep_minute',
        bash_command='sleep 60',
        dag=dag
    )

    def day_of_week():
        weekno = datetime.today().weekday()
        if weekno < 5:
            print("Weekday")
        else:
            print("Weekend")

    weekday = PythonOperator(
        task_id='weekday',
        python_callable=day_of_week,
        dag=dag
    )

    def test_func():
        logging.info("test")


    test = PythonOperator(
        task_id='test',
        python_callable=test_func,
        dag=dag
    )

    dummy >> [echo_ds, test, sleep_minute] >> weekday
