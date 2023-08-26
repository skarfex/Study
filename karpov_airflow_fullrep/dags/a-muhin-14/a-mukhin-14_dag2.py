"""
Загрузка статьи из GP
"""
from datetime import datetime
from airflow import DAG
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1, 4),
    'end_date': datetime(2022, 3, 14, 4),
    'owner': 'a-mukhin-14',
    'poke_interval': 600
}

with DAG("a-mukhin-14_dag2",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-mukhin-14']
         ) as dag:
    start_task = DummyOperator(task_id='start')


    def get_article(week_day, **kwargs):
        pg_hook = PostgresHook('conn_greenplum')
        id = datetime.strptime(week_day, '%Y-%m-%d').isoweekday()
        logging.info(f"Getting text of the article {id}")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {id}')
        query_res = cursor.fetchone()[0]
        conn.close()
        logging.info(f"Getting text of the article '{query_res}'")
        kwargs['ti'].xcom_push(value=query_res, key='heading')


    get_text = PythonOperator(
        task_id='get_article',
        op_kwargs={'week_day': "{{ ds }}"},
        python_callable=get_article,
        provide_context=True
    )

    start_task >> get_text
