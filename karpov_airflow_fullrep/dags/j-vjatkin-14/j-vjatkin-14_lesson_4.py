"""
Simple DAG for Airflow with connection to GreenPlum instance
by Yuri Vyatkin, 2022
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime.datetime(2022, 3, 1),
    'end_date': datetime.datetime(2022, 3, 14),
    'owner': 'j-vjatkin',
    'poke_interval': 600
}

with DAG("j-vjatkin-14_lesson_4",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['j-vjatkin']
) as dag:

    def get_article(article_id):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("gp_conn")
        cursor.execute(f"select heading from articles where id = {article_id};")
        result = cursor.fetchone()[0]
        logging.info("Returned: " + result)

    get_article = PythonOperator(
        task_id='get_article',
        python_callable=get_article,
        op_args=['{{ dag_run.logical_date.weekday() + 1 }}', ],
        dag=dag
    )

    get_article
