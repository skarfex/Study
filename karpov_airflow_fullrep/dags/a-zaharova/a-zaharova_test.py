from airflow import DAG
from airflow.utils.dates import days_ago

import logging
import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-zaharova',
    'poke_interval': 600
}
with DAG("a-zaharova",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-zaharova']
         ) as dag:
    dummy = DummyOperator(task_id="dummy")
    echo_a = BashOperator(
        task_id='echo_a',
        bash_command='date +"%d-%m-%y"'
    )


    def today_date_func():
        logging.info(datetime.datetime.now().strftime("%d-%m-%y"))


    today_date = PythonOperator(
        task_id='today_date',
        python_callable=today_date_func
    )
    dummy >> [echo_a, today_date]
