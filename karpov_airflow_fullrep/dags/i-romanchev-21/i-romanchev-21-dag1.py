"""
DAG для выбора заголовка статьи по дню недели
"""
from datetime import datetime
from airflow import DAG
import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1, 3, 0, 1),
    'end_date': datetime(2022, 3, 15, 3, 0, 1),
    'owner': 'i-romanchev-21'
}

with DAG("i-romanchev-21-dag1",
         default_args=DEFAULT_ARGS,
         schedule_interval='1 3 * * 1-6',
         max_active_runs=1,
         tags=['i-romanchev-21']
         ) as dag:
    def select_article_heading_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        weekday = datetime.strptime(kwargs['ds'], '%Y-%m-%d').weekday() + 1
        logging.info("weekday is {}".format(weekday))
        cursor.execute('SELECT heading FROM articles WHERE id = {};'.format(weekday))
        query_res = cursor.fetchall()
        logging.info("QUERY_RES is: {}".format(query_res))
        # one_string = cursor.fetchone()[0]
        # logging.info("ONE_STRING is {}".format(one_string))


    select_article_heading = PythonOperator(
        task_id="python",
        python_callable=select_article_heading_func
    )
