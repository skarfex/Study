"""
Этот DAG или показывает время на момент запуска функции Check_seconds через Python
или время выполнения Bash-скрипта
"""

from airflow import DAG
import datetime,time
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator

DEFAULT_ARGS = {
    'owner': 'a-krysanov-18',
    'start_date': datetime.datetime(2023, 3, 4),
    'end_date': datetime.datetime(2023, 3, 6)
}

with DAG('a-krysanov-18-lesson3',
    schedule_interval='*/5 * * * *',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-krysanov-18']) as dag:
    
    now = datetime.datetime.now()
    Unix_now = int(time.mktime(now.timetuple()))
    
    dummyOp = DummyOperator(task_id="start")

    def Check_seconds(seconds=Unix_now):
        if seconds % 2 == 0:
            return 'py_operator'
        return 'bash_operator'

    options = BranchPythonOperator(
    task_id='fork',
    python_callable=Check_seconds
    )
    
    def time_checked(t=now):
        return print(t)
    
    py_operator = PythonOperator(
        task_id='py_operator',
        python_callable=time_checked
    )
    
    bash_operator = BashOperator(
        task_id='bash_operator',
        bash_command="printf '%(%Y-%m-%d %H:%M:%S)T\n'"
    )
    
    dummyOp >> options >> [py_operator, bash_operator]
