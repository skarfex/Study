from airflow import DAG
import pendulum
import logging

from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'start_date': pendulum.datetime(2023, 3, 1, tz='utc'),
    'end_date': pendulum.datetime(2023, 3, 14, tz='utc'),
    'owner': 's-nikulina-20',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    's-nikulina-20_lesson_4',
    description='My lesson_4 DAG',
    schedule_interval='@daily',
    default_args=default_args,
    tags=['s-nikulina-20'],
    max_active_runs=1,
    catchup=True
)


def get_week_day(str_date):
    return pendulum.from_format(str_date, 'YYYY-MM-DD').weekday() + 1


def is_weekday(**context):
    week_day = get_week_day(context['ds'])
    return week_day not in (6, 7)


def get_article(**context):
    week_day = get_week_day(context['ds'])
    get_article_sql = f"SELECT heading FROM articles WHERE id = {week_day}"
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(get_article_sql)
    query_res = cursor.fetchone()
    logging.info(query_res[0])
    return query_res[0]


is_weekday_task = ShortCircuitOperator(
    task_id='is_weekday_task',
    python_callable=is_weekday,
    dag=dag
)

get_article_task = PythonOperator(
    task_id='get_article_task',
    python_callable=get_article,
    dag=dag
)

is_weekday_task >> get_article_task








