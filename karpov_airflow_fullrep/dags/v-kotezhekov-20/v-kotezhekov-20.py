"""
Dag для уроков 3-4
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import datetime as dt

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': dt.datetime(2022, 3, 1),
    'end_date': dt.datetime(2022, 3, 14),
    'owner': 'v-kotezhekov-20',
    'poke_interval': 600
}

with DAG("v-kotezhekov-20_dag",
    # schedule_interval='@daily',
    schedule_interval='0 0 * * 1-6',

    default_args=DEFAULT_ARGS,
    max_active_runs=1,

    tags=['v-kotezhekov-20']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )


    def get_heading(**kwargs):

        execution_date = kwargs['execution_date']
        weekday = execution_date.strftime('%w')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute('SELECT heading FROM articles WHERE id = %s', (weekday,)) # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        # one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info(f"Execution date: {execution_date}")
        logging.info(f"Weekday: {weekday}")
        logging.info(f"Query result: {query_res}")




    print_python = PythonOperator(
        task_id='python_ds',
        python_callable=get_heading,
        dag=dag
    )

    dummy >> [echo_ds, print_python]

