"""
Тестовый даг
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's-badaraeva-8',
    'poke_interval': 600
}

with DAG("s-badaraeva-8",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['s-badaraeva-8']
          ) as dag:
    Dummy = dummyoperator(task_id = "dummy")
    
  echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

def Hello_World_func():
    logging.info("Hello World")


Hello_World = PythonOperator(
    task_id='Hello World',
    python_callable=Hello_world,
    dag=dag
)
dummy>> {echo_ds,Hello_World}