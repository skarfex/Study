"""
Lesson 3 dag
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'ed-kuzhamuratov',
    'poke_interval': 600
}

with DAG("ed-kuzhamuratov_lesson3",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['ed-kuzhamuratov']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo "Today is {{ execution_date }}"',
        dag=dag
    )

    def hello_world_func():
        logging.info("Hello World!")

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )

    def f_is_friday(exec_dt):
        exec_day = datetime.strptime(exec_dt, '%Y-%m-%d').weekday()
        is_friday = (exec_day == 4)
        return is_friday

    friday_only = ShortCircuitOperator(
        task_id='friday_only',
        python_callable=f_is_friday,
        op_kwargs={'exec_dt': '{{ ds }}'},
        dag=dag
    )

    friday_celebration = BashOperator(
        task_id='friday_celebration',
        bash_command='echo "Today is Friday, hooray!"',
        dag=dag
    )

    dummy >> echo_ds >> hello_world >> friday_only >> friday_celebration