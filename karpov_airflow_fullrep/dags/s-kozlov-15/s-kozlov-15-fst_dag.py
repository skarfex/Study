"""
Тестовый DAG
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime
import locale
locale.setlocale(locale.LC_ALL, '')

from airflow.operators.dummy import DummyOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import ShortCircuitOperator

DEFAULT_ARGS = {
    'start_date': days_ago(7),
    'owner': 's-kozlov-15',
    'poke_interval': 600
}

with DAG("sk_dag_lesson_3",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['sk_lesson_3']
         ) as dag:

    dummy = DummyOperator(task_id='dummy')

    echo_ds = BashOperator(
        task_id = 'calday_sapf',
        bash_command='echo {{ ds_nodash }}',
        dag = dag
    )

    def new_lesson_checkday():
        now = datetime.now().strftime("%A")
        if datetime.today().isoweekday() in (1, 3, 5):
            logging.info(f'Сегодня {now} - новый урок =)')

    check_lesson = PythonOperator(
        task_id='check_lesson',
        python_callable=new_lesson_checkday,
        dag=dag
    )

    def if_weekend(exec_dt):
        exec_day = datetime.strptime(exec_dt, '%Y-%m-%d').isoweekday()
        return exec_day in [6, 7]

    weekend_run = ShortCircuitOperator(
        task_id='weekend_run',
        python_callable=if_weekend,
        op_kwargs={'exec_dt': '{{ds}}'}
    )

    def weekend_check():
        logging.info('Выходные? Делаем домашку! %)')


    check_vih = PythonOperator(
        task_id='weekend_check',
        python_callable=weekend_check,
        dag=dag
    )

    dummy >> [echo_ds, check_lesson] >> weekend_run >> check_vih