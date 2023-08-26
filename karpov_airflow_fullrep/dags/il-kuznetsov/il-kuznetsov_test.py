from airflow import DAG
from datetime import datetime
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 2),
    'end_date': datetime(2022, 3, 15),
    'owner': 'il-kuznetsov',
    'poke_interval': 600
}

with DAG("il-kuznetsov-test-3-heading",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['il-kuznetsov']
          ) as dag:

    start = DummyOperator(task_id="start")

    def taking_one_string_from_db_func(article_id):
        article_id = article_id.replace('"', '')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        logging.info(f'_________________ WHERE id = {article_id}')
        cursor.execute(f'SELECT heading FROM articles WHERE id = {article_id}')
        return cursor.fetchone()[0]


    taking_one_string_from_db = PythonOperator(
        task_id='taking_one_string_from_db',
        python_callable=taking_one_string_from_db_func,
        op_args=['{{ dag_run.logical_date.weekday() + 1 }}', ],
        do_xcom_push=True,
        dag=dag
    )

    start >> taking_one_string_from_db
