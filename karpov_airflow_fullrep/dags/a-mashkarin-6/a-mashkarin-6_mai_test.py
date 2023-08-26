from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

DEFAULT_ARGS = {
    'start_date': days_ago(7),
    'owner': 'a-mashkarin-6'
}

dag = DAG('a-mashkarin-6_mai_test',
          schedule_interval='@hourly',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-mashkarin-6'])

dummy_task = DummyOperator(task_id='dummy_operator',
                           dag=dag)

bash_task = BashOperator(task_id='bash_operator',
                         bash_command='date',
                         dag=dag)

def print_date():
    print(datetime.today())

python_task = PythonOperator(
    task_id='print_date',
    python_callable=print_date,
    dag=dag,
)

dummy_task >> [bash_task, python_task]
