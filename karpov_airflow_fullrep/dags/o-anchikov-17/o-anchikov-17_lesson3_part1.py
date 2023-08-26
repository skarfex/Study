"""
Lesson 3
"""
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'o-anchikov-17',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(seconds=5),
     'poke_interval': 300,
    'trigger_rule': 'all_success'
}

with DAG("o-anchikov-17_lesson3_part1",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['o-anchikov-17_lesson3_part1']
) as dag:

    dummy_start = DummyOperator(task_id="dag_start")
    dummy_end = DummyOperator(task_id="dag_end")

    bash_counter = BashOperator(
        task_id='bash_counter',
        bash_command='sleep 0.5 && echo “I’ve been sleeping for 0.5 seconds, I want more” && sleep 0.5 && echo “I’m done sleeping, thanks!”',
        dag=dag
    )

    def hello_world_func():
        time = datetime.today()
        print ('hello_world! timestamp logged!')
        logging.info("current_date: "+str(time))

    python_op = PythonOperator(
        task_id='python_task',

        python_callable=hello_world_func,
        dag=dag
    )

    dummy_start >> bash_counter >> python_op >> dummy_end