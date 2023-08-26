from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from datetime import datetime

import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'do-bokarev'
}

with DAG("do-bokarev-simple-branch",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['do-bokarev']
        ) as dag:

    dummy = DummyOperator(task_id="dummy")

    logging_str = 'Current Date: {{ ds }}'

    def print_template_func(print_this):
        logging.info(print_this)

    branch_even_str = "This is even branch"
    branch_odd_str = "This is odd branch"

    branch_even = PythonOperator(task_id="branch_even", python_callable=print_template_func, op_args=[branch_even_str])

    branch_odd = PythonOperator(task_id="branch_odd", python_callable=print_template_func, op_args=[branch_odd_str])

    def checkifeven_func(exec_dt):
        day_of_month=int(datetime.strptime(exec_dt, '%Y-%m-%d').day)
        return "branch_even" if day_of_month % 2 ==0 else "branch_odd"

    always_run = PythonOperator(task_id="branch_always",python_callable=print_template_func, op_args=[datetime.today().strftime('yyyy-MM-dd')])

    checkifeven = BranchPythonOperator(task_id="checkifeven",
                                       python_callable=checkifeven_func,
                                       op_kwargs={'exec_dt':'{{ ds }}'})




    dummy >> checkifeven >> [branch_even , branch_odd] >> always_run


