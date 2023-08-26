from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 's-abdibekov-17',
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    's-abdibekov-17_3',
    default_args=default_args,
    schedule_interval='0 0 * * 1-6',  # every day from Monday to Saturday
)

def get_article_heading(**kwargs):
    ds = kwargs['execution_date'].strftime('%w')  # get day of the week (0-6)
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    query = "SELECT heading FROM articles WHERE id = '{}'".format(ds)
    cursor.execute(query)
    query_res = cursor.fetchone()[0]
    logging.info("Article heading: {}".format(query_res))
    return query_res

run_this = PythonOperator(
    task_id='get_article_heading',
    python_callable=get_article_heading,
    provide_context=True,
    dag=dag,
)



