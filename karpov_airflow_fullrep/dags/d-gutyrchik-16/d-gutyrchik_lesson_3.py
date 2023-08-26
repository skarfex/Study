from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
import logging

DEFAULT_TASK_ARGS = {
    'owner': 'dgutyrchik',
    'start_date': datetime(2022, 3, 2),
    'end_date': datetime(2022, 3, 15),
}

def get_article_heading(**kwargs):
    date = kwargs['execution_date']
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor("cursor")
    cursor.execute(f"select heading from articles a where id = {date.weekday()}")
    query_res = cursor.fetchall()
    logging.info(query_res)

with DAG(
        dag_id="d-gutyrchik-16_lesson3",
        schedule_interval="0 1 * * 1,2,3,4,5,6",
        default_args=DEFAULT_TASK_ARGS,
        max_active_runs=1,
        catchup=True,
        tags=["dgutyrchik"],
) as dag:
    get_heading = PythonOperator(
        task_id="get_heading",
        python_callable=get_article_heading,
        provide_context=True,
        dag=dag
    )

get_heading
