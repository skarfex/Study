"""
Тестовый даг - задание к уроку 4
"""

import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook


def task_success_alert(context):
    print(f"My DAG has succeeded, run_id: {context['run_id']}")

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'owner': 'e-sidorkina-11',
    'poke_interval': 600,
    'on_success_callback': task_success_alert
}

with DAG("e-sidorkina-11_second_dags",
         schedule_interval='10 10 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         catchup=True,
         tags=['e-sidorkina-11']
         ) as dag:
    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def postgre_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()+1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(exec_day))
        query_result = cursor.fetchall()
        return query_result

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=postgre_func,
        op_kwargs={'execution_dt': '{{ ds }}'},
        dag=dag
    )

    dummy >> [echo_ds, load_data]