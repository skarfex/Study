from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime.datetime(2022, 3, 1),
    'end_date': datetime.datetime(2022, 3, 14),
    'owner': 'm-rjazanskij-15',
    'poke_interval': 600
}

with DAG("rjazanskij_2_task_dag",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['rjazanskij']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def get_query_res():
        day_of_week = datetime.datetime.today().weekday() + 1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        print(query_res)


    get_res = PythonOperator(
        task_id='get_query_result',
        python_callable=get_query_res,
        dag=dag
    )

    dummy >> [echo_ds, get_res]