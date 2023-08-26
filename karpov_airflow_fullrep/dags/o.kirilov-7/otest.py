"""
Тестовый даг
"""
#Import libraries
from airflow import DAG                                         #DAG functions
from airflow.utils.dates import days_ago                        #daterime functions
import logging                                                  #log functions

#Operators import
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

#default args
DEFAULT_ARGS = {
    'start_date': days_ago(2),                                  #first start date
    'owner': 'okr',                                             #owner DAG
    'poke_interval': 600                                        #refresh time
}

#Metadate DAG
with DAG("ok_test",                                             #dag name
    schedule_interval='@daily',                                 #sheduler
    default_args=DEFAULT_ARGS,                                  
    max_active_runs=1,                                          #run count
    tags=['okkk']                                               #tag
)   as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def hello_world_func():
        logging.info("Hello World!")

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )

    dummy >> [echo_ds, hello_world]