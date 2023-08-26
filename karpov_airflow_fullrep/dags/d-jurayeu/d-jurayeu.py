"""
Просто даг с выводом дат при помощи разных операторов
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'd-jurayeu',
    'poke_interval': 600
}

dag = DAG("d-jurayeu-3",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['d-jurayeu']
          )

dummy_task = DummyOperator(
    task_id='dummy_task',
    dag=dag
)

wait_until_8am_task = TimeDeltaSensor(
    task_id='wait_until_8am_task',
    delta=timedelta(seconds=8*60*60),
    dag=dag
)

echo_ds_task = BashOperator(
    task_id='echo_ds_task',
    bash_command='echo {{ ds }}',
    dag=dag
)


def python_date():
    return datetime.today()


python_date_task = PythonOperator(
    task_id='python_date_task',
    python_callable=python_date,
    dag=dag
)


dummy_task >> wait_until_8am_task >> [echo_ds_task, python_date_task]
