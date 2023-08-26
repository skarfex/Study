	"""
Тестовый даг Кабанова К.
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'k-kabanova-21',
    'poke_interval': 600
}

with DAG("kkabanova_test",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['k-kabanova']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_kkabanova',
        bash_command='echo {{ ds }}'
    )
    template_str = dedent(""" ds: {{ ds }}""")

    def print_template_func(print_this):
        print(print_this)

    print_ds = PythonOperator(
        task_id='print_ds',
        python_callable=print_template_func,
        op_args=[template_str]
    )

    dummy >> [echo_ds, print_ds]