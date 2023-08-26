"""
Тестовый даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import pendulum

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1, tz="UTC"),
    'end_date': pendulum.datetime(2022, 3, 14, tz="UTC"),
    'owner': 'e-sologub',
    'poke_interval': 600
}

with DAG("e-sologub_task_4",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['e-sologub']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )


    def is_not_sunday_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return exec_day not in [6]


    is_not_sunday = ShortCircuitOperator(
        task_id='weekend_only',
        python_callable=is_not_sunday_func,
        op_kwargs={'execution_dt': '{{ds}}'}
    )

    def get_article_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday() + 1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")
        cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(exec_day))
        one_string = cursor.fetchone()[0]
        logging.info(one_string)

    get_article = PythonOperator(
        task_id='get_article',
        python_callable=get_article_func,
        op_kwargs={'execution_dt': '{{ ds }}'},
        dag=dag
    )

    dummy >> echo_ds >> is_not_sunday >> get_article