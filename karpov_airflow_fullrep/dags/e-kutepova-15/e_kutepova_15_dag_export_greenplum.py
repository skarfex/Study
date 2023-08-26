"""
Забираем данные из GreenPlum (поле articles из таблицы heading) и выводим результат в лог
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import datetime

from airflow.hooks.postgres_hook import PostgresHook
import logging
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'e-kutepova-15',
    'poke_interval': 600
}

with DAG("e-kutepova-15.dag_export_greenplum",
        schedule_interval='0 0 * * 1-6',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['e-kutepova-15']
        ) as dag:

    dummy_start = DummyOperator(task_id='start_task')

    dummy_end = DummyOperator(task_id='end_task')

    def export_from_greenplum_func(*args, **kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")

        weekday = datetime.datetime.today().weekday() + 1
        cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(weekday))
        query_result = cursor.fetchall()

        logging.info(f'Query result for {weekday}: {query_result}')
        kwargs['ti'].xcom_push(value=query_result, key='article')

    export_from_greenplum = PythonOperator(
        task_id='export_from_greenplum',
        python_callable=export_from_greenplum_func
    )

    dummy_start >> export_from_greenplum >> dummy_end