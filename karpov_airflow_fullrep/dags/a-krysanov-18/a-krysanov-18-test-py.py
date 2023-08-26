from airflow import DAG
from airflow.utils.dates import days_ago
import logging
#test
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import get_current_context

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-krysanov-18',
    'poke_interval': 300
}

with DAG('a-krysanov-18-test',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-krysanov-18']
) as dag:
 
    dummyOp = DummyOperator(task_id="dummy")

    bashOp = BashOperator(
        task_id='bashOp',
        bash_command='echo BashOperator is working properly!'
    )

    def python_op_func():
        logging.info('PythonOperator is working properly')
        print(get_current_context())

    pythonOp = PythonOperator(
        task_id='pythonOp',
        python_callable=python_op_func,
    )

    dummyOp >> bashOp >> pythonOp