"""
Даг, посвященный заданию 3-го урока из блока "ETL"
Раз в день в 12:15 по Гринвичу с помощью BashOperator даг выводит дату исполнения в различных форматах,
a с помощью PythonOperator даг выводит дату вчерашнего дня и дату дня, который был 5 дней назад
"""

from airflow import DAG
import logging
import textwrap as tw
import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

# Функция, отображающая в лог успешное завершение таска
def success_task(context):
    logging.info('________________________________________________________________')
    logging.info("Таск завершился успешно!")
    logging.info('________________________________________________________________')

DEFAULT_ARGS = {
    'owner': 'o-bazan',
    'start_date': datetime.datetime(2023,4,1),
    'on_success_callback': success_task
}

dag = DAG(
    dag_id = "o-bazan_lesson3_dag",
    schedule_interval='15 12 * * *', # каждый день 12:15
    default_args=DEFAULT_ARGS,
    tags=['lesson3', 'o-bazan']
)

# Строка-входной параметр для execution_date_task
str_template = tw.dedent("""
______________________________________________________________
One day ago was {{ macros.ds_add(ds, -1) }}
Five days ago was {{ macros.ds_add(ds, -5) }}
______________________________________________________________
""")

# Функция вывода в лог str
def print_str_func(str_template):
   logging.info(str_template)


start_task = DummyOperator(
    task_id="start_task",
    dag=dag
)

execution_date_task = BashOperator(
    task_id='execution_date_task',
    bash_command='echo execution_date: {{ execution_date }}\n echo ds: {{ ds }}\n echo ds_nodash: {{ ds_nodash }}\n echo ts: {{ ts }}',
    dag=dag
)

today_date_task = PythonOperator(
    task_id='today_date_task',
    python_callable=print_str_func,
    op_args=[str_template],
    dag=dag
)

start_task >> execution_date_task >> today_date_task

