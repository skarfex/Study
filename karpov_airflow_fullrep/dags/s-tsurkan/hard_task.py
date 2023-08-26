from airflow import DAG
import logging
from datetime import datetime, timedelta

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'owner': 's-tsurkan',
    'retries': 1,
    'poke_interval': 10,
    'retry_delay': timedelta(seconds=5)
}

with DAG("hard-task",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    schedule_interval='0 0 * * 1-6',
    tags=['s-tsurkan'],
    catchup=True
    ) as dag:

    start = DummyOperator(task_id="start")

    def get_heading_articles(weekday):
        logging.info("start loading...")
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        query = f'SELECT heading FROM articles WHERE id = {weekday}'
        cursor.execute(query)
        query_res = cursor.fetchall()
        logging.info(query_res[0])
        return query_res[0]

    get_data = PythonOperator(
        task_id='get_data',
        provide_context=True,
        python_callable=get_heading_articles,
        op_args=['{{ dag_run.logical_date.weekday() + 1 }}'],
        do_xcom_push=True
    )

    end = DummyOperator(task_id="end")

    start >> get_data >> end