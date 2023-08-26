import logging
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'owner': 'a-lunev-21',
    'start_date': days_ago(1)
}

def print_date_func(ds):
    logging.info(ds)


with DAG(
    dag_id='a-lunev-21-dag_1',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-lunev']
) as dag:

    task_1 = DummyOperator(task_id="task_1")

    task_2 = BashOperator(
        task_id='task_2',
        bash_command='echo {{ ds }}'
    )

    task_3 = PythonOperator(
        task_id='task_3',
        python_callable=print_date_func,
    )

    task_1 >> task_2 >> task_3
