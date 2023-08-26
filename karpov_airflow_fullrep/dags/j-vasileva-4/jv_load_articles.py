"""
A DAG for Lesson 4, part 2.
"""

from airflow import DAG
from airflow.utils.dates import datetime
from airflow.operators.python import get_current_context
import logging

from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'j-vasileva-4',
    'poke_interval': 600
}

with DAG("jv_load_articles",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['j-vasileva-4']
         ) as dag:

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}'
    )

    def load_articles_from_gp_func():
        context = get_current_context()
        this_date = context['ds']
        day_of_week = datetime.strptime(this_date, '%Y-%m-%d').weekday() + 1
        gp_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = gp_hook.get_conn()
        cursor = conn.cursor('named_cursor_name')
        cursor.execute(f'SELECT HEADING FROM ARTICLES WHERE ID = {day_of_week}')
        query_res = cursor.fetchall()
        logging.info(f"Heading Data: {query_res}")

    load_articles_from_gp = PythonOperator(
        task_id='load_articles_from_gp',
        provide_context=True,
        python_callable=load_articles_from_gp_func
    )

echo_ds >> load_articles_from_gp

dag.doc_md = __doc__
echo_ds.doc_md = """Пишет в лог execution_date"""
