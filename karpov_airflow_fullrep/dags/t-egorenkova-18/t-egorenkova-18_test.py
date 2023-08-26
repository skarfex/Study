"""
Тестовый даг
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 't-egorenkova-18',
    'poke_interval': 600
}

with DAG('t-egorenkova-18_test',
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['t-egorenkova-18']
         ) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(task_id="echo_ds",
                           bash_command="echo {{ ds }}",
                           dag=dag)

    def return_date():
        return datetime.now().date()

    first_task = PythonOperator(task_id="first_id",
                                python_callable=return_date,
                                dag=dag)

    dummy >> echo_ds >> first_task


