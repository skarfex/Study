
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import time

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'g-zatulo',
    'poke_interval': 600
}

with DAG("g-zatulo",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['g-zatulo']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo `date +%m-%d-%Y`',
        dag=dag
    )

    def date_func():
        logging.getLogger().setLevel(logging.INFO)
        logging.info(time.strftime('%m/%d/%Y'))

    date_py = PythonOperator(
        task_id='date',
        python_callable=date_func,
        dag=dag
    )

    dummy >> [echo_ds, date_py]