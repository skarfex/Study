"""
My Dag
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
import datetime

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's-jakovleva-20',
    'poke_interval': 600
}

with DAG("ys_dag",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['s-jakovleva-20']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )
    date_today = datetime.datetime.now()
    date_format = "%d.%m.%Y"
    def data_today():
        logging.info(date_today.strftime(date_format))

    data_pyt = PythonOperator(
        task_id='data_pyt',
        python_callable=data_today,
        dag=dag
    )

    dummy >> [echo_ds, data_pyt]