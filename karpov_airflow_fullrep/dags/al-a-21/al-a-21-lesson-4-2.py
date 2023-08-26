from airflow import DAG
import logging
import pendulum

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1, tz="UTC"),
    'end_date': pendulum.datetime(2022, 3, 14, tz="UTC"),
    'owner': 'al-a-21'
}

with DAG('al-a-21-lesson-4.2',
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['al-a-21']
) as dag:

    def get_week_day(date_string):
        weekday = pendulum.from_format(date_string, "YYYY-MM-DD").weekday()
        return weekday + 1

    def get_articles_func(**kwargs):
        week_day = get_week_day(kwargs['ds'])
        get_article_sql = f"SELECT heading FROM articles WHERE id = {week_day}"
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(get_article_sql)
        query_res = cursor.fetchone()
        logging.info(query_res[0])

    get_articles = PythonOperator(
        task_id='get_articles',
        python_callable=get_articles_func
    )

    end = DummyOperator(task_id="end")

    get_articles >> end