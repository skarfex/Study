"""
Даг 3 урок
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'pavel-khavanski',
    'poke_interval': 600
}

dag_second = DAG(
    "second_dag",
    start_date=days_ago(0, 0, 0, 0, 0),
    tags=['pavel-khavanski'],
    default_args=DEFAULT_ARGS,
    schedule_interval='@weekly'

)

operation_1 = BashOperator(
    bash_command="pwd",
    dag=dag_second,
    task_id='operation_1'

)

operation_2 = BashOperator(
    bash_command="""echo "need check wheather" """,
    dag=dag_second,
    task_id='operation_2'

)


def good_wheather():
    logging.info("PythonOperator: Today is a good wheather")

operation_3 = PythonOperator(
    python_callable=good_wheather,
    dag=dag_second,
    task_id='operation_3'
)

operation_4 = BashOperator(
    bash_command="""echo "completed" """,
    dag=dag_second,
    task_id='operation_4'
)



operation_1 >> operation_2 >> operation_3 >> operation_4
