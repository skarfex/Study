"""
Читаем статьи из БД
Airflow DAG. Lesson 4.
"""

from airflow import DAG
import logging
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 's-gerasimov-17',
    'poke_interval': 600,
    'retries': 3
}

with DAG("s-gerasimov-17-lesson4",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['s-gerasimov-17']
         ) as dag:
    dummy_start = DummyOperator(task_id="dummy_start")

    def read_from_db():
        weekday = datetime.today().weekday()
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        try:
            cursor.execute(f"SELECT heading FROM articles WHERE id = {weekday}")
            query_res = cursor.fetchall()
            # one_string = cursor.fetchone()[0]
            logging.info("Result is ", query_res)
        except:
            logging.error("Some error occurred ._.")

    read_data = PythonOperator(
        task_id="read_article_data",
        python_callable=read_from_db,
        dag=dag
    )

    dummy_end = DummyOperator(task_id="dummy_end")

    dummy_start >> read_data >> dummy_end
