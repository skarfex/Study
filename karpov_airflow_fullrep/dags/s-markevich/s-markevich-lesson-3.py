"""
karpov courses
lesson 3
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import timedelta
from datetime import datetime as dt

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),  # дата начала генерации DAG Run-ов
    'owner': 's-markevich',     # в ладелец
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG("s-markevich-lesson-3",  # название такое же, как и у файла для удобной ориентации в репозитории
    schedule_interval='@daily',  # расписание
    default_args=DEFAULT_ARGS,   # дефолтные переменные
    max_active_runs=1,           # позволяет держать активным только один DAG Run
    tags=['s-markevich']       # тэги
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',             # уникальный идентификатор таски внутри DAG
        bash_command='echo {{ ds }}',  # выполняемый bash script (ds = execution_date) дата нашего теста
        dag=dag                        # в этом варианте нужно явно прописывать, к какому DAG относится задача
    )

    def date_now_func():
        logging.info(f"data now: {dt.now()}")

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=date_now_func,  # ссылка на функцию, выполняемую в рамках таски
        dag=dag
    )

    # зависемости не делаю специально
