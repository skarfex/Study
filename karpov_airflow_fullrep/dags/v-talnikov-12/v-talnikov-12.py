"""
Тестовый даг
Отрабатывает каждый день с 2021-01-01 по 2025-01-01
Запускается в 7 вечера по НСК (UTC +7) = полдень по UTC
Пишет в лог и информирует, что время пришло...
В случае падения пытается запуститься 2 раза
"""
from datetime import datetime
from logging import info

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'owner': 'v-talnikov-12',
    'retries': 2,
    'start_date': datetime(2021, 1, 1),
    'end_date': datetime(2025, 1, 1),
}


with DAG(
        dag_id="v-talnikov-12_simple_dag",
        schedule_interval='0 12 * * *',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['v', 'v-talnikov-12']
) as dag:

    def notification_func():
        print('Время пришло...')
        info("The time has come...")


    notification_time = PythonOperator(
        task_id='notification_time',
        python_callable=notification_func,
        dag=dag
    )

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo The time has come {{ ds }}',  # выполняемый bash script (ds = execution_date)
        dag=dag
    )

    check = DummyOperator(
        task_id='check',
        trigger_rule='none_failed',
        dag=dag)

    echo_ds >> notification_time >> check
