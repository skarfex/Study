from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's-aliev',
    'poke_interval': 600
}

with DAG(
    dag_id='s-aliye_dag',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['s-aliev']
) as dag:

    dummy = DummyOperator(task_id='dummy')

    today_date = BashOperator(
        task_id='today_date',
        bash_command='echo {{ ds }}',
        dag=dag
    )

dummy >> today_date
