import random
import string
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

dag = DAG(
    dag_id="dz-uraeva-14-hw-3",
    schedule_interval="15 0 * * *",
    start_date=datetime(2022, 11, 10, 18, 00),
    max_active_runs=1,
    tags=["karpov", "dz-uraeva-14"]
)


def print_random_string_func():
    letters = string.ascii_letters
    print(''.join(random.choice(letters) for i in range(10)))


def select_random_task_func():
    return random.choice(['print_random_string', 'print_ds'])


start = DummyOperator(task_id='start', dag=dag)

print_random_string = PythonOperator(
    task_id='print_random_string',
    python_callable=print_random_string_func,
    dag=dag
)

print_ds = BashOperator(
    task_id='print_ds',
    bash_command='echo "ds={{ ds }}"',
)

select_random_task = BranchPythonOperator(
    task_id='select_random_task',
    python_callable=select_random_task_func,
    dag=dag
)

end = DummyOperator(task_id='end', dag=dag)

start >> select_random_task >> [print_random_string, print_ds] >> end
