"""
Тестовый даг Рузов А.В.
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a.ruzov',
    'email': ['ruzov.anton@yandex.ru'],
    'poke_interval': 600
}


with DAG("a.ruzov_test",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a.ruzov']
) as dag:

    # Созадание пустого оператора. Нужен для объединения операторов?
    dummy = DummyOperator(task_id="dummy")

    # Оператор, который выводит текст
    """
    Этот оператор не отрабатывается...почему???
    echo_ds = BashOperator(
        task_id='echo_aruzov',
        bash_command='echo -n "Bash command works fine"',
        dag=dag
    )
    """

    # Оператор-функция
    def hello_world_func():
        for i in range(5):
            logging.info(f"Hello World! ... {i}")

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )

    dummy >> hello_world
