"""
TEST
"""
import pendulum
import logging
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 6, 10, tz="UTC"),
    'owner': 'a-juferov'
}


def test_func(**kwargs):
    try:
        print('2')
        for x in os.listdir(os.path.join(os.path.dirname(__file__), '../../')):
            print(x)
    except:
        pass

    try:
        print('3')
        for x in os.listdir(os.path.join(os.path.dirname(__file__), '../../plugins')):
            print(x)
    except:
        pass


with DAG("a-juferov_test_files",
     schedule_interval=None,
     default_args=DEFAULT_ARGS,
     max_active_runs=1,
     tags=['a-juferov']) as dag:

    test = PythonOperator(
        task_id='test',
        python_callable=test_func
    )

    test
