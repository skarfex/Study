'''3 урок. Сложные пайпланы. Часть 1.
Создать даг из нескольких тасков:
— DummyOperator
— BashOperator с выводом даты
— PythonOperator с выводом даты'''

from datetime import date

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's-polusmakov',
    'poke_interval': 600
}

with DAG("psv_2L3",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['s-polusmakov']
         ) as dag:

    def date_psv():
        logging.info(f"{date}")

    dummy = DummyOperator(task_id="dummy")

    date_bash = BashOperator(
        task_id='date_psv',
        bash_command='echo {{ ds }}',
        # dag=dag
    )

    date_py = PythonOperator(
        task_id='date_py',
        python_callable=date_psv,
        # dag=dag
    )

    dummy >> [date_bash, date_py]


