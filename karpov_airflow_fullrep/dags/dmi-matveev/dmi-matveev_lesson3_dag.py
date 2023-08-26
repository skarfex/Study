"""
Практическое задание по уроку 3.
В рамках урока 3 создана цепочка тасков:
- start (пустой таск, просто стартует даг)
- wait_five_minute (сенсор который ждет 5 минут)
- random_and_check_number (таск ветвления, вызывает функцию которая генерирует число в диапазоне от 0 до 9 и проверять его
на четность/нечетность. В зависимости от результата четности/нечетности случайного числа возвращает следующий таск в цепочке)
- even_number (таск вызываемый в случае четности числа, записывает в лог строку "Четное число")
- odd_number (таск вызываемый в случае нечетности числа, записывает в лог строку "Нечетное число")
"""

from airflow import DAG
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy import DummyOperator
import logging
from random import randint
from datetime import datetime, timedelta

DEFAULT_ARGS = {
    'owner': 'matveevdm',
    'start_date': datetime(2023, 1, 15),
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

with DAG(dag_id='dmi-matveev_lesson3_dag',
         schedule_interval='@hourly',
         default_args=DEFAULT_ARGS,
         tags=['matveevdm']
         ) as dag:

    wait_five_minute = TimeDeltaSensor(task_id='wait_five_minute',
                                       delta=timedelta(seconds=60 * 5)
                                       )


    def random_number():
        number = randint(0, 9)
        logging.info(f'Рандом вернул число - {number}')
        if number % 2 == 0:
            return 'even_number'
        else:
            return 'odd_number'


    random_and_check_number = BranchPythonOperator(task_id='random_and_check_number',
                                                   python_callable=random_number
                                                   )


    def check_number_even():
        logging.info('Четное число')


    even_number = PythonOperator(task_id='even_number',
                                 python_callable=check_number_even
                                 )


    def check_number_odd():
        logging.info('Нечетное число')


    odd_number = PythonOperator(task_id='odd_number',
                                python_callable=check_number_odd
                                )

    start = DummyOperator(task_id='start')

    start >> wait_five_minute >> random_and_check_number >> [even_number, odd_number]

    dag.doc_md = __doc__
    start.doc_md = """Таск ничего не делает, просто связывает цепочки задач"""
    wait_five_minute.doc_md = """Сенсор ожидает 5 минут"""
    random_and_check_number.doc_md = """Таск исполняет функцию Python, которая генерирует случайное число от 0 до 9
    и проверяет его на четность/нечетность, в зависимости от результата проверки определяется
    следующий таск в цепочке (even_number или odd_number)"""
    even_number.doc_md = """Таск пишет в лог 'Четное число'"""
    odd_number.doc_md = """Таск пишет в лог 'Нечетное число'"""
