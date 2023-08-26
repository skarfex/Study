"""
a-martyshkin-15
Урок 3 - тестовый даг
"""
from airflow import DAG
from datetime import datetime
import locale
import pendulum
import logging

import datetime as dt

from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook #  хук для работы с GP
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'owner': 'a-martyshkin-15',
    'start_date': pendulum.now(tz='utc'),
    'poke_interval': 120
}

with DAG('martyshkin_lesson_3',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-martyshkin-15']
        ) as dag:

    def get_week_day(ds):
        return pendulum.from_format(ds, 'YYYY-MM-DD').weekday()

    def func_workdays(ds):
        if get_week_day(ds) not in (5, 6):
            return 'work'
        else:
            return 'weekend'


    weekday_branch = BranchPythonOperator(
        task_id='weekday_branch',
        python_callable=func_workdays,
        dag=dag
    )

    def test_function(ds):
        logging.info(F"It's {__import__('calendar').day_name[get_week_day(ds)]}, I have to get up for work =(")

    echo_ds = BashOperator(
        task_id='Now',
        bash_command='''echo {{ ds_nodash }}
        echo {{ ds }}
        echo {{ data_interval_start  }}
        echo {{ data_interval_end  }}
        echo {{ ts  }}''',
        dag=dag
    )

    def info(ds):
        print(F"Today is {__import__('calendar').day_name[get_week_day(ds)]} and you can sleep =)")

    weekend = PythonOperator(
        task_id='weekend',
        python_callable=info
    )

    work = PythonOperator(
        task_id='work',
        python_callable=test_function
    )

    echo_ds >> weekday_branch >> [work, weekend]