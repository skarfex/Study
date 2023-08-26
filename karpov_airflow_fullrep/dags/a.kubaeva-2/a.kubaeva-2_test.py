"""
Тестовый даг
"""

import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a.kubaeva-2',
    'poke_interval': 600
}


with DAG(dag_id="a.kubaeva-2-test",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a.kubaeva-2']) as dag:

    def func_print():
        logging.info("First str")

    dummy = DummyOperator(task_id="dummy")

    python_task = PythonOperator(
        task_id='python_task_1',
        python_callable=func_print,
        dag=dag
        )

    dummy >> python_task
