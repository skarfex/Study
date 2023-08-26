from airflow import DAG
import logging
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'owner': 't-panyutina',
    'start_date': days_ago(1),
    'poke_interval': 600
}

with DAG('t_panyutina_test',
         schedule_interval = '@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['panyutina']) as dag:

    dummy_1 = DummyOperator(task_id='dummy1')

    dummy_2 = DummyOperator(task_id='dummy_2')

    def test_python():
        logging.info('test')

    test_python = PythonOperator(
        task_id='test_python',
        python_callable=test_python,
        dag=dag
    )

    [dummy_1, dummy_2] >> test_python

