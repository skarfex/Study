"""
Тестовый даг
"""
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
import logging
import os



DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm-rjabushkin',
    'poke_interval': 600
}

@dag(
    "m-rjabushkin_test_v2",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['m-rjabushkin']
)


def start():

    @task
    def dummy():
        pass

    @task
    def bash():
        os.system('echo {{ ds }}')

    @task
    def hello_world_func():
        logging.info("Hello World!")

    dummy() >> [bash(), hello_world_func()]

a = start()