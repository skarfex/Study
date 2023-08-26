"""
Dag для урока 3: сложные пайплайны. Содержит 3 задачи: пустышка, питон-оператор и баш-оператор,
которые выводят даты
"""

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

import logging
from datetime import datetime

DEFAULT_ARGS = {
    'start_date': datetime(2023, 6, 7),
    'owner': 'g-igumnov',
    'poke_interval': 600
}

dag = DAG("g-igumnov_4",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['iga']
          )

dummy_op = DummyOperator(task_id='dummy_op', dag=dag)

bash_out_date = BashOperator(
    task_id='bash_out_date',
    bash_command='echo {{ds}}',
    dag=dag
    )

def print_date():
    date_exec = datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
    logging.info(date_exec)

py_out_date = PythonOperator(
    task_id='py_out_date',
    python_callable=print_date,
    dag=dag
)

dummy_op >> [bash_out_date, py_out_date]
