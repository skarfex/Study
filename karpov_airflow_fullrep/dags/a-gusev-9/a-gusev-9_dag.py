"""
Простой DAG
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-gusev-9',
    'poke_interval': 600
}

with DAG("a-gusev-9_dag",
         schedule_interval='@hourly',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-gusev-9']
         ) as dag:

    dummy = DummyOperator(task_id='dummy')

    all_gusev_a = BashOperator(
        task_id='all_gusev-a',
        bash_command='cut -d: -f1 /etc/passwd | sort | grep "a-gusev-\d" > /tmp/a-gusev-9',
        dag=dag
    )

    def del_impostor_func():
        with open('/tmp/a-gusev-9', 'r') as file_in:
            lines = file_in.readlines()
        for l in lines:
            if l != 'a-gusev-9':
                logging.info("We should del impostor {l} !\n".format(l=l))

    del_impostor = PythonOperator(
        task_id='del_impostor',
        python_callable=del_impostor_func,
        dag=dag
    )

    dummy >> all_gusev_a >> del_impostor
