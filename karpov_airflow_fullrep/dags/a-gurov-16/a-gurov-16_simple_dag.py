from airflow import DAG
from datetime import timedelta
from datetime import datetime
from airflow.sensors import time_delta_sensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'start_date': days_ago(5),
    'owner': 'a-gurov-16'
}

def print_hello():
    print('Hello')

dag = DAG("a-gurov-16_simple_dag",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1
          )

t1 = BashOperator(
        task_id='echo_hi',
        bash_command='echo "Hello"',
        dag=dag
    )

t2 = PythonOperator(
        task_id='print_smth',
        python_callable=print_hello,
        dag=dag
    )

t1 >> t2