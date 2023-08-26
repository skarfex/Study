from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-borgatin'
}

with DAG('a-borgatin-14',
         default_args=DEFAULT_ARGS,
         schedule_interval='0 1 * * *',
         tags=['a-borgatin']) as dag:
    dummy = DummyOperator(task_id='dummy')

    print_date = BashOperator(task_id='print_date',
                              bash_command='echo {{ ds }}')


    def print_hello():
        print("Hello, Airflow!")


    print_task = PythonOperator(task_id='print_hello',
                                python_callable=print_hello)

    dummy >> print_date >> print_task
