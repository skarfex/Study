"""
Module 2 Class 3(+ продолжение class 4)
Даг состоит из баш-оператора,
питон-оператора
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import pendulum
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a-zakirova-17',
    'poke_interval': 600,
    'retries': 3,
    'depends_on_past': False,
    'start_date': pendulum.datetime(2022, 3, 1, tz="UTC"),
    'end_date': pendulum.datetime(2022, 3, 14, tz="UTC")
}

with DAG("a-zakirova-17_module2",
         schedule_interval='0 0 * * *',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-zakirova-17']
         ) as dag:
    dummy_start = DummyOperator(task_id="dummy_start")

    def is_not_sunday_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return exec_day not in [6]


    not_sunday = ShortCircuitOperator(
        task_id='weekend_only',
        python_callable=is_not_sunday_func,
        op_kwargs={'execution_dt': '{{ds}}'}
    )


    def get_articles_func(ds):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        query = 'SELECT heading FROM articles WHERE id = {weekday}'.format(
            weekday=datetime.strptime(ds, '%Y-%m-%d').isoweekday())
        cursor.execute(query)
        query_res = cursor.fetchall()
        logging.info("Articles heading with this weekday id: {print_res}".format(print_res=query_res))


    get_articles = PythonOperator(
        task_id='get_articles',
        python_callable=get_articles_func,
        op_kwargs={'execution_dt': '{{ds}}'}
    )

    dummy_start >> not_sunday >> get_articles
