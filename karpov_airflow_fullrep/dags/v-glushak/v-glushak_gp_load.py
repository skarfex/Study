from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import csv
from datetime import datetime, date

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_ARGS = {
    'owner': 'glushak',
    'poke_interval': 600,
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'email': ['v.yu.glushak@tmn2.etagi.com'],
    'email_on_failure': True
}


with DAG("glushak_load_articles_gp",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['glushak'],
          catchup=True,
) as dag:


    def load_gp_articles(current_day):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        weekday = date.fromisoformat(current_day).weekday() + 1
        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')
        query_res = cursor.fetchall()
        logging.info(f"current_day {current_day}")
        logging.info(f"weekday {weekday}")
        logging.info(f"query_res {query_res}")

    load_data_gp = PythonOperator(
        task_id='load_gp_articles',
        python_callable=load_gp_articles,
        op_args=['{{ ds }}']
    )

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    start >> load_data_gp >> end
