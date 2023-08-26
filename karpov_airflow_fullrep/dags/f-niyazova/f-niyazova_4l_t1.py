'''Собираем данные из гринплам каждый день кроме воскресенья '''

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'f-niyazova',
    'poke_interval': 600,
    'depends_on_past': False
}

with DAG('f-n_4l_1t',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=50,
    tags=['f-niyazova'],
    catchup= True,
) as dag:

    def is_not_sunday_func(execution_dt, **kwargs):
        exec_day = datetime.strptime(execution_dt, "%Y-%m-%d").weekday()
        return exec_day in [0, 1, 2, 3, 4, 5]

    is_not_sunday = ShortCircuitOperator(
        task_id='is_not_sunday',
        python_callable=is_not_sunday_func,
        op_kwargs={'execution_dt': '{{ ds }}'}
    )    

    def load_gp_data_func(execution_dt, **kwargs):
        day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday() + 1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day}')  # исполняем sql
        one_string = cursor.fetchone()[0]
        return one_string

    load_gp_data = PythonOperator(
        task_id='load_gp_data',
        python_callable=load_gp_data_func,
        op_kwargs={'execution_dt': '{{ ds }}'}
        )

    is_not_sunday >> load_gp_data