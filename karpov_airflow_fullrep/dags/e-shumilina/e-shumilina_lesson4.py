"""
Даг из 4 урока
"""
from airflow import DAG
import logging
from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'e-shumilina',
    'poke_interval': 600,
    'catchup': False
}

with DAG("e-shumilina_lesson4",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['e-shumilina']
          ) as dag:

    def get_heading(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        weekday = kwargs['execution_date'].weekday() + 1

        cursor.execute(f'select heading from articles where id = {weekday})')
        result = cursor.fetchone()[0]

        logging.info(f"current weekday: {weekday}")
        logging.info(f"data from Greenplum: {result}")

    get_data = PythonOperator(
        task_id='get_heading',
        python_callable=get_heading,
        dag=dag
    )

    get_data