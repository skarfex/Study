'''Тестовый DAG'''


from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
     'owner': 'n-ryzhkov-14',
    'start_date': days_ago(2),
    'retries': 3
}

with DAG ('n-ryzhkov-14_test',
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['n-ryzhkov-14']
          ) as dag:
    start = DummyOperator(task_id='start')

    date_ts = BashOperator(
        task_id='date_ts',
        bash_command='echo {{ ts }}',
        dag=dag
    )

    def some_text_func():
        logging.info('some text')

    some_text = PythonOperator(
        task_id='some_text',
        python_callable=some_text_func,
        dag=dag
    )

    start >> date_ts >> some_text