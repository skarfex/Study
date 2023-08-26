from airflow import DAG
from airflow.utils.dates import days_ago
import datetime
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'd-nikolaev-27',
    'poke_interval': 600
}

with DAG("d-nikolaev-27-dag3",
    schedule_interval='0 0 * * MON-SAT',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['d-nikolaev-27-dag3']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def hello_world_func():
        datetime_object = datetime.datetime.now()
        logging.info(datetime_object)

    echo_ds_python = PythonOperator(
        task_id='echo_ds_python',
        python_callable=hello_world_func,
        dag=dag
    )

    dummy >> [echo_ds, echo_ds_python]