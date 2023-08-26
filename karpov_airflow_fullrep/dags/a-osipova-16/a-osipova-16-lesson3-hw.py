"""
HW. Lesson 3. More complex pipeline. Part 1.
"""

from airflow import DAG
from datetime import timedelta, datetime
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'owner': 'a-osipova-16',
    'email': ['zwinglyx@yandex.ru'],
    'email_on_failure': True,
    'email_on_retry': True,
    'depends_on_past': False,
    'wait_for_downstream': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2023, 1, 1),
    'end_date': datetime(2023, 1, 31),
    'sla': timedelta(hours=2)
}

with DAG(
    dag_id="a-osipova-16-lesson3-hw",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-osipova-16']
) as dag:

    task_1 = DummyOperator(task_id="task_1")

    task_2 = BashOperator(
        task_id='task_2',
        bash_command='echo Execution Date: {{ ds }}',
        dag=dag
    )

    def logs_func(**kwargs):
        logging.info('--------------------------------')
        logging.info('templates_dict, task_owner: ' + kwargs['templates_dict']['task_owner'])
        logging.info(f'op_args, {{ ds }}: ' + kwargs['ds'])
        logging.info('--------------------------------')

    task_3 = PythonOperator(
        task_id='task_3',
        python_callable=logs_func,
        templates_dict={'task_owner': '{{ task.owner }}'},
        provide_context=True
    )

    task_1 >> task_2 >> task_3


dag.doc_md = __doc__