from airflow.decorators import dag
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import logging


@dag(dag_id='m.popov_9_less_3', schedule_interval=None, start_date=days_ago(1))
def my_dag():
    task_1 = DummyOperator(
        task_id='task_1'
    )

    task_2 = BashOperator(
        task_id='task_2',
        bash_command='echo {{ts}}'
    )

    def print_date():
        logging.info('{{ts}}')

    task_3 = PythonOperator(
        task_id='task_3',
        python_callable=print_date
    )

    task_1 >> task_2 >>task_3

my_dag = my_dag()