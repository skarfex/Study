"""
Потапов Сергей
Тестовый даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from datetime import timedelta, datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(7),
    'owner': 's_potapov_11',
    'poke_interval': 600
}


with DAG("s-potapov-11_lesson_4",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['s_potapov_11']
) as dag:

    def get_data_from_articles_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute('SELECT heading FROM articles WHERE id = {day}'.format(
            day=datetime.strptime(kwargs['ds'], '%Y-%m-%d').weekday() + 1))

        query_res = cursor.fetchone()
        kwargs['ti'].xcom_push(key='article_of_the_day', value=query_res)





    get_data_from_articles = PythonOperator(
        task_id='get_data_from_articles',
        python_callable=get_data_from_articles_func,
        provide_context=True,
        dag=dag
    )

    # dummy >> [echo_ds, hello_world]
    #

    get_data_from_articles