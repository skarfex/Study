"""
Даг, определяющий будний день или выходной.
Даг состоит из 2 операторов:
 - bash-оператора (выводит дату)
 - python-оператора (выводит информацию будний день или выходной)
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(10),
    'owner': 'e-pogrebetskaja-10',
    'poke_interval': 600
}

with DAG("lenush_first_dag",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['e-pogrebetskaja-10']
         ) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def print_day_info(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        if exec_day in [5, 6]:
            logging.info(execution_dt + " is a weekend :)")
        else:
            logging.info(execution_dt + " is a workday :(")

    print_day_information = PythonOperator(
        task_id='day_info',
        python_callable=print_day_info,
        op_kwargs={'execution_dt': '{{ ds }}'},
        dag=dag
    )

    dummy >> [echo_ds, print_day_information]

    dag.doc_md = __doc__

    echo_ds.doc_md = """Пишет в лог execution_date"""
    print_day_info.doc_md = """Пишет информацию о том будний день или выходной"""
