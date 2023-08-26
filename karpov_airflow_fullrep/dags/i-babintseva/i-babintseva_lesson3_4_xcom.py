"""
This DAG extract articles from GreenPlum by id = week day (1-Mon...6-Sat)
"""
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1, 0, 0, 0),
    'end_date': datetime(2022, 3, 14, 3, 0, 0),
    'owner': 'i-babintseva'
}

with DAG('i-babintseva_lesson3_4_xcom',
         schedule_interval='0 3 * * 1-6',
         max_active_runs=1,
         default_args=DEFAULT_ARGS,
         tags=['i-babintseva'],
         ) as dag:
    dummy = DummyOperator(task_id="dummy")

    echo_date = BashOperator(
        task_id='echo_date',
        bash_command='echo {{ ds }}'
    )

    def print_date_func(**kwargs):
        logging.info(kwargs['ds'])
        logging.info(kwargs['ts'])
        kwargs['ti'].xcom_push(value=kwargs['ds'], key='day')

    print_date = PythonOperator(
        task_id='print_date',
        python_callable=print_date_func,
        provide_context=True
    )

    def get_articles_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        day = kwargs['ti'].xcom_pull(task_ids='print_date', key='day')
        article_id = datetime.strptime(day, '%Y-%m-%d').weekday() + 1
        cursor.execute(f'SELECT heading FROM articles WHERE id = {article_id}')

        logging.info('--------------')
        logging.info('Day ' + day)
        for query_res in cursor:
            logging.info(f'Article heading by id {article_id}: {query_res}')
        logging.info('--------------')

    get_articles_from_gp = PythonOperator(
        task_id='get_articles_from_gp',
        python_callable=get_articles_func,
        provide_context=True
    )

    dummy >> echo_date >> print_date >> get_articles_from_gp