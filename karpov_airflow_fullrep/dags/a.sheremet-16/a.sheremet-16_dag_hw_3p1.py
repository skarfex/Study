# == Урок 3. Задание 1 ==============================================
# 1. Скачать репозиторий.
# 2. Настроить IDE для работы с ним.
# 3. Создать в папке dags папку по имени своего логина в Karpov LMS.
# 4. Создать в ней питоновский файл, начинающийся со своего логина.
# 5. Внутри создать даг из нескольких тасков, на своё усмотрение:
#    — DummyOperator
#    — BashOperator с выводом строки
#    — PythonOperator с выводом строки
#    — любая другая простая логика
# 6. Запушить даг в репозиторий.
# 7. Убедиться, что даг появился в интерфейсе airflow и отрабатывает без ошибок.

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, get_current_context
from datetime import timedelta
from datetime import datetime
from random import shuffle

default_args = {
    'owner': 'a.sheremet-16',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 18),
    'schedule_interval': '0 9 * 1 1-6'
}

dag = DAG('sam_dag_hw_3p1'
          , catchup=False
          , default_args=default_args
          , tags=['sheremet_am'])
    
start = DummyOperator(task_id = 'start', dag=dag)
end = DummyOperator(task_id = 'end', dag=dag)
    
sam_echo_ds = BashOperator(
    task_id='sam_echo_ds'
    , bash_command= 'echo Result at {{ ds }}'
    , dag=dag)


def _print_func():
    return print(f'Hello World!')

def _counting_out_func():
    foo = ['one potato'
           , 'two potatoes'
           , 'three potatoes'
           , 'four'
           , 'five potatoes'
           , 'six potatoes'
           , 'seven potatoes'
           , 'more'
           , 'bad one']
    shuffle(foo)
    return print(f"Counting out is: {foo[0]}")

sam_print_task = PythonOperator(
    task_id='sam_print_task'
    , python_callable=_print_func
    , dag=dag)

sam_counting_task = PythonOperator(
    task_id='sam_counting_task'
    , python_callable=_counting_out_func
    , dag=dag)
    
start >> sam_echo_ds >> sam_print_task >> sam_counting_task >> end