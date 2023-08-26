""" (03) Автоматизация ETL-процессов >> 3 урок >> Задания

Внутри создать даг из нескольких тасков, на своё усмотрение:
• DummyOperator
• BashOperator с выводом строки
• PythonOperator с выводом строки
• любая другая простая логика

"""

from airflow.exceptions import AirflowSkipException
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

with DAG(
    dag_id = 'i-derkach-16-03-03.0',
    start_date = datetime(2023, 1, 20),
    schedule_interval = '@daily',
    max_active_runs = 10,
    catchup = True,
    tags=['i-derkach']
) as dag:

    for op, args in {
        BashOperator: {"bash_command": "date +\"%Y-%m-%dT%H:%M:%S%z\""},
        PythonOperator: {"python_callable": lambda: print(datetime.now().isoformat())}
    }.items():
        task_id = op.__name__.lower().replace('operator', '')
        group = DummyOperator(task_id = f'group_{task_id}_op')
        operate = op(task_id=f"get_{task_id}_datetime", **args)
        group >> operate


