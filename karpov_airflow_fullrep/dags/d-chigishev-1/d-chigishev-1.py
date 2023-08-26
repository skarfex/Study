"""
Task 4-1: Load data from DB
"""

from airflow import DAG
# from airflow.utils.dates import datetime
from datetime import datetime
import logging
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'd-chigishev-1',
    'poke_interval': 600
}


with DAG("d-chigishev-1",
          schedule_interval='1 3 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['d-chigishev-1']
          ) as dag:

    def get_heading_from_db(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles '
                       f'WHERE id = {datetime.strptime(kwargs["ds"], "%Y-%m-%d").weekday() + 1}')
        one_string = cursor.fetchone()[0]
        logging.info(one_string)

    get_heading = PythonOperator(
        task_id='get_heading',
        python_callable=get_heading_from_db,
        dag=dag
    )

    get_heading


