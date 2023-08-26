"""
Дз airflow 3 урок
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'k-nameverchenko-19',
    'poke_interval': 180
}

with DAG(
    dag_id="kv_test_3",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['k-nameverchenko-19']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def b_k_func():
        logging.info("Bambarbiya Kergudu!")

    bambarbiya_kergudu = PythonOperator(
        task_id='bambarbiya_kergudu',
        python_callable=b_k_func,
        dag=dag
    )

    dummy >> [echo_ds, bambarbiya_kergudu]