from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.dates import days_ago
from datetime import timedelta


default_args={
    'owner':'m-rjabushkin',
    'retries':5,
    'retry_delay':timedelta(minutes=5),
    'start_date':days_ago(5),
    'schedule_interval':'@daily'
}

def greet(name, age):
    print(f'Hello {name} and {age}')


with DAG(
    'm-rjabushkin_les3',
    default_args=default_args,
    tags=['m-rjabushkin_lesson3']
) as dag: 

    task1=TimeDeltaSensor(
        task_id='First_Task_Wait_Time',
        delta=timedelta(seconds=5)
    )

    task2=BashOperator(
        task_id='Second_Task',
        bash_command='echo {{ ds }}',
        trigger_rule='one_success')

    task3=PythonOperator(
        task_id='Hello',
        python_callable=greet,
        op_kwargs={'name':'Maksim','age':25}
    )
    
task1>>task2>>task3
