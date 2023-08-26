from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v-kiojbash',
    'poke_interval': 600
}

def print_date():
    print(f'Today is {datetime.today().strftime("%Y-%m-%d")}')

with DAG('v-kiojbash_lesson3',
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['v-kiojbash']
         ) as dag:

    dummy_task = DummyOperator(task_id='dummy_task')

    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "Today is {{ ds }}"'
    )

    python_task = PythonOperator(
        task_id='python_task',
        python_callable=print_date
    )

    dummy_task >> [bash_task, python_task]