from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator

DEFAULT_ARGS = {
    'start_date': days_ago(7),
    'owner': 'l-fokina-17',
    'poke_interval': 600
}

with DAG("fridays_party",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['l-fokina']
) as dag:

    start_task = DummyOperator(task_id = "start_task")

    def is_friday_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return exec_day in [4]

    today_friday = ShortCircuitOperator(task_id='today_friday',
                                        python_callable=is_friday_func,
                                        op_kwargs={'execution_dt': '{{ ds }}'}
                                        )

    def go_to_patry_func():
        logging.info("Go to party!")

    go_to_party = PythonOperator(task_id='go_to_party',
                                 python_callable=go_to_patry_func)

    start_task >> today_friday >> go_to_party