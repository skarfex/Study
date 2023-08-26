from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'sduzhak',
    'poke_interval': 600
}

with DAG("sduzhak",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['sduzh']
         ) as dag:
    dummy = DummyOperator(task_id="dummy")

    echo = BashOperator(
        task_id='echo',
        bash_command='echo {{ ds }}'
    )

    def print_date(**kwargs):
        date = kwargs.get('date')
        print(date)

    get_date = PythonOperator(
        task_id='get_date',
        python_callable=print_date,
        op_kwargs={'date': '{{ ds }}'}
    )

    dummy >> echo >> get_date

    def non_sunday_f(execution_date, **kwargs):
        # custom_date = '2022-03-11'
        exec_day = datetime.strptime(execution_date, '%Y-%m-%d').weekday()
        kwargs['ti'].xcom_push(value=(exec_day+1), key="weekday")
        return exec_day != 6

    non_sunday = ShortCircuitOperator(
        task_id='non_sunday',
        python_callable=non_sunday_f,
        op_kwargs={'execution_date': '{{ ds }}'}
    )

    def select_header_f(exec_date, **kwargs):
        weekday = kwargs['ti'].xcom_pull(task_ids='non_sunday', key='weekday')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("cursor_date")
        cursor.execute(f"""
            SELECT heading FROM articles WHERE id = {str(weekday)}
            """
        )
        one_row = cursor.fetchone()[0]
        print(f'{exec_date} :: {one_row}')

    select_header = PythonOperator(
        task_id='header',
        python_callable=select_header_f,
        op_kwargs={"exec_date": "{{ ds }}"}
    )

    dummy >> non_sunday >> select_header