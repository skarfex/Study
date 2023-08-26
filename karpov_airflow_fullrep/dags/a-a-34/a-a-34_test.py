"""
Тестовый даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-a-34',
    'poke_interval': 600
}

with DAG('a-a-34_test',
    schedule_interval='0 0 * * MON,TUE,WED,THU,FRI,SAT',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-a-34']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def show_row_from_greenplum_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute('SELECT heading FROM articles WHERE id = 1')  # исполняем sql
        # query_res = cursor.fetchall()  # полный результат
        one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info(one_string)

    show_row_from_greenplum = PythonOperator(
        task_id='show_row_from_greenplum',
        python_callable=show_row_from_greenplum_func,
        dag=dag
    )

    dummy >> [echo_ds, show_row_from_greenplum]
