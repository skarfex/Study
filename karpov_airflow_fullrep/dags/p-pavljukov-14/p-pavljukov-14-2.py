"""
Дорабатываем даг для работы по расписанию
"""

from airflow import DAG
from datetime import timedelta, datetime
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1, 10, 00),
    'end_date': datetime(2022, 3, 14, 10, 00),
    'owner': 'p-pavljukov-14',
    'poke_interval': 400
}

with DAG(
        dag_id='p-pavljukov-14-2',
        schedule_interval="0 10 * * 1-6",
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['p-pavljukov-14']
        ) as dag:

    start_task = DummyOperator(task_id='start_task')

    def greenplum_func(execution_date):
        y = datetime.strptime(execution_date, "%Y-%m-%d")
        x = y.isoweekday()
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute('SELECT heading FROM articles WHERE id={0}'.format(x))
        one_string = cursor.fetchone()[0]
        logging.info(one_string)

    python_task = PythonOperator(
        task_id='python_task',
        python_callable=greenplum_func,
        op_kwargs={'execution_date': '{{ ds }}'},
        dag=dag
    )

    start_task >> python_task