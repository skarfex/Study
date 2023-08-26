"""
Lesson 4
"""
from airflow import DAG
import logging
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'owner': 'a-loskutov-2',
    'poke_interval': 600
}

with DAG("a-loskutov-lesson-4",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['Loskutov']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    def get_greenplum_func(**kwargs):
        ds = str(kwargs['ds'])
        dt = datetime.strptime(ds, "%Y-%m-%d")
        day_num = dt.isoweekday()

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_num}')
        one_string = cursor.fetchone()[0]
        logging.info(one_string)

    get_greenplum = PythonOperator(
        task_id='get_greenplum',
        python_callable=get_greenplum_func,
        provide_context=True,
        dag=dag
    )

    dummy >> get_greenplum


