"""
Базовая структура графа (DAG)
"""


import logging
import pendulum as pdl

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# параметры графа
args = dict(
    dag_id='lf-simple',

    schedule_interval='@daily',
    max_active_runs=1,
    catchup=False,

    tags=['leo', 'fedyaev', 'simple', 'custom', 'dag'],

    default_args={
        'start_date': pdl.today('UTC').add(days=-2),
        'owner': 'l-fedjaev-13', # airflow login
        'poke_interval': 600,
    },
)

# созадать граф
with DAG(**args) as dag:
    info = BashOperator(task_id='info', bash_command='echo {{ run_id }}')
    pwd = BashOperator(task_id='pwd', bash_command='pwd')
    ls = BashOperator(task_id='ls', bash_command='ls')
    stamp = PythonOperator(task_id='stamp',
                           python_callable=lambda: print('¿Can you see this?'))

    info >> pwd >> ls >> stamp
