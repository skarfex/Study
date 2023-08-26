"""
Тестовый даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2), # дата начала генерации DAG Run-ов
    'owner': 'n-novikova-16',  # в ладелец
    'poke_interval': 600       # задает интервал перезапуска сенсоров (каждые 600 с.)
}

# Работа через контекстный менеджер
# DAG не нужно указывать внутри каждого таска, он назначается им автоматически
with DAG("n-novikova-lesson-2", # название такое же, как и у файла для удобной ориентации в репозитории
    schedule_interval='@daily', # расписание
    default_args=DEFAULT_ARGS,  # дефолтные переменные
    max_active_runs=1,          # позволяет держать активным только один DAG Run
    tags=['n-novikova-16']      # тэги
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds', # уникальный идентификатор таски внутри DAG
        bash_command='echo {{ ds }}', # выполняемый bash script (ds = execution_date) дата нашего теста
        dag=dag # в этом варианте нужно явно прописывать, к какому DAG относится задача
    )

    def hello_world_func():
        logging.info("Hello World!")

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func, # ссылка на функцию, выполняемую в рамках таски
        dag=dag
    )

    dummy >> [echo_ds, hello_world]