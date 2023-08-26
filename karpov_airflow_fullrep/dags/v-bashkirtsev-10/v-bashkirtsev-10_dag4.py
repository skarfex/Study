"""
DAG для 4 урока
Забираю из GP данные и вывожу в лог
"""

from airflow import DAG
import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'v-bashkirtsev-10',
    'poke_interval': 600
}


def get_data(ds):
    """Читаю данные из бд"""

    request = 'SELECT heading FROM articles WHERE id = {id}'.format(
        id=datetime.strptime(ds, '%Y-%m-%d').isoweekday()
    )
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(request)
    query_res = cursor.fetchall()
    for item in query_res:
        logging.info('Article heading: {title}'.format(title=item[0]))


def log_res(ds):
    """логирование результата"""

    log_str = 'Date: {dt}, week day number: {day_number}.'.format(
        dt=ds,
        day_number=datetime.strptime(ds, '%Y-%m-%d').isoweekday(),
    )
    logging.info(log_str)


with DAG("v-bashkirtsev_gp",
         schedule_interval='59 23 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['v-bashkirtsev-10']
         ) as dag:

    get_article = PythonOperator(
        task_id='get_article',
        python_callable=get_data,
        dag=dag
    )

    log_date = PythonOperator(
        task_id='log_date',
        python_callable=log_res,
        dag=dag
    )

    get_article >> log_date
