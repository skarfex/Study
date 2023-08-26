"""
j-ibragimov-15 lesson 3 1st part
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import random
import datetime


from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'owner': 'j-ibragimov-15',
    'poke_interval': 600
    
}

def christmas_tree():
    x = datetime.date.today().day
    logging.info("\n" + "\n".join([f"{'*' * (2 * n + 1):^{2 * x + 1}}" for n in (*range(x), 0, 0)]))

with DAG("yi_lesson3",
    schedule_interval='@daily',
    start_date=days_ago(0) ,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['j-ibragimov-15'],
    user_defined_macros={'christmas_tree': christmas_tree}
) as dag:

    dummy = DummyOperator(
        task_id='upstream_check',
        trigger_rule='all_success'
    )

    christmas_tree = PythonOperator(
        task_id='christmas_tree',
        python_callable=christmas_tree,
        dag=dag
    )

    weekday = BashOperator(
        task_id='weekday',
        bash_command="echo Today number of week is {{execution_date.strftime('%w')}}",
        dag=dag
    )
     

    christmas_tree >> weekday >> dummy