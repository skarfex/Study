from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import date
import logging
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
'start_date': days_ago(1),
'owner': 'Karpov',
'poke_interval': 600
}

dag = DAG(
'azamat_dag_1',
schedule_interval='@daily',
default_args=DEFAULT_ARGS,
max_active_runs=1,
tags=['karpov']
)

eod = DummyOperator(
task_id='eod',
trigger_rule='one_success',
dag=dag
)

echo_ds = BashOperator(
task_id='echo_ds',
bash_command='echo {{ ds }}',
dag=dag
)

def date_func():
    return date.today()

date_today = PythonOperator(
task_id='date_today',
python_callable=date_func,
dag=dag
)

echo_ds >> date_today >> eod