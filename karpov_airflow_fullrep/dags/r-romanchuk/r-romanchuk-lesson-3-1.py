from airflow import DAG
import logging
from airflow.utils.dates import days_ago
from datetime import timedelta

from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.python import PythonOperator

from datetime import datetime
from datetime import date

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'end_date': datetime(2023, 1, 1),
    'email': ['r.romanchuk@ya.ru'],
    'email_on_failure': True,
    'depends_on_past': False,
    'poke_interval': 600,
    'owner': 'r-romanchuk'
}

with DAG("r-romanchuk-lesson-3-1",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1
         ) as dag:

    wait_until_6am = TimeDeltaSensor(
        task_id='wait_until_6am',
        delta=timedelta(seconds=6 * 60),
        dag=dag
    )

    def today_date_func():
        print('Today is ' + str(date.today()))

    today_date = PythonOperator(
        task_id='today_date',
        python_callable=today_date_func,
        dag=dag
    )

    def summer_func():
        dt = date.today()
        if dt.month in [6, 7, 8]:
            logging.info("It's summer!")
        else:
            logging.info("Waiting for summer...")

    summer = PythonOperator(
        task_id='summer_check',
        python_callable=summer_func,
        dag=dag
    )

    wait_until_6am >> [today_date, summer]
