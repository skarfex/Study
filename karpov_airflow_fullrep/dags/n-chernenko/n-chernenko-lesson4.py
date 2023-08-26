"""
Тестовый даг урок4
n-chernenko
"""

from airflow import DAG
import logging
from datetime import datetime
import pendulum

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'owner': 'n-chernenko',
    'poke_interval': 60,
    'start_date': pendulum.datetime(2022, 3, 1, tz="UTC"),
    'end_date': pendulum.datetime(2022, 3, 14, tz="UTC")
}

with DAG("n-chernenko_lesson4",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['n-chernenko']
         ) as dag:

    start = BashOperator(
        task_id='start',
        bash_command='echo "Now: {{ ds }}"',
        dag=dag
    )

    def query_articles_func(current_date, **kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        day_of_week = datetime.strptime(current_date, '%Y-%m-%d').isoweekday()
        logging.info(f'execution weekday: {day_of_week}')
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')
        query_res = cursor.fetchall()
        kwargs['ti'].xcom_push(value=query_res, key='heading')
        logging.info(str(query_res))

    query_articles = PythonOperator(
        task_id='query_articles',
        op_kwargs={'current_date': "{{ ds }}"},
        python_callable=query_articles_func,
        provide_context=True,
        dag=dag
    )

    start >> query_articles
