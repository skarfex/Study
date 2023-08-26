"""
Getting articles' headings from Greenplum DB and showing their in the logs

"""
from airflow import DAG
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'a-bodrov',
    'poke_interval': 600
}


def get_article_haddings_func(ds, **kwargs):
    request = 'SELECT heading FROM articles WHERE id = {id}'.format(
        id = datetime.strptime(ds, '%Y-%m-%d').isoweekday()
    )
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(request)
    query_res = cursor.fetchall()
    for item in query_res:
        logging.info('Article heading: {title}'.format(title=item[0]))

def log_date_func(ds, **kwargs):
    log_str = 'Date: {dt}, week day number: {day_number}.'.format(
        dt = ds,
        day_number = datetime.strptime(ds, '%Y-%m-%d').isoweekday(),
    )
    logging.info(log_str)


with DAG (
    dag_id="a-bodrov_get_articles",
    schedule_interval='59 23 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-bodrov']
) as dag:

    get_article_haddings = PythonOperator(
        task_id='get_article_haddings',
        python_callable=get_article_haddings_func,
        dag=dag
    )

    log_date = PythonOperator(
        task_id='log_date',
        python_callable=log_date_func,
        dag=dag
    )

    get_article_haddings >> log_date