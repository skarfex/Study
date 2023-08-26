from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    'owner': 'r-kurbanova',
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14)
}


def hello():
    return "Hello, my friend"


with DAG("r-kurbanova",
         schedule_interval='@once',
         catchup=True,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['r-kurbanova']
         ) as dag:
    make_hello = PythonOperator(
        task_id='make_hello',
        python_callable=hello
    )

    end_dummy = DummyOperator(
        task_id='end_dummy'
    )

    make_hello >> end_dummy
