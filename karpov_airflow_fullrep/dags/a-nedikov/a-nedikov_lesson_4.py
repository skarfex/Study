from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, date, time
import logging

DEFAULT_ARGS = {
    'owner': 'a-nedikov',
    'start_date': datetime(2022,3,1),
    'end_date':datetime(2022,3,14),
    'retries': 3,
    'poke_interval': 600
}

with DAG(
    dag_id='a-nedikov_dag2',
    default_args=DEFAULT_ARGS,
    description='MySecondDAG',
    schedule_interval='@once',
    max_active_runs=1,
    tags=['a-nedikov'],
) as dag:

    def get_week_day(date):
        return datetime.strptime(date, '%Y-%m-%d').weekday() + 1

    def check_day_func(**kwargs):
        return get_week_day(kwargs['ds']) != 7

    def read_article_pg_func(**kwargs):
        week_day = get_week_day(kwargs['ds'])
        get_article_sql = f"SELECT heading FROM articles WHERE id = {week_day}"

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(get_article_sql)
        query_res = cursor.fetchone()
        logging.info(query_res[0])

    check_day = PythonOperator(
        task_id='check_day',
        python_callable=check_day_func,
        provide_context=True
    )

    read_article = PythonOperator(
        task_id='pg_data_reader',
        python_callable=read_article_pg_func
    )

    check_day >> read_article