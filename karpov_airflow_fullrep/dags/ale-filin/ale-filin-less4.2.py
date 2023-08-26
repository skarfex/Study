from airflow import DAG
import pendulum
import logging

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1, tz='utc'),
    'end_date': pendulum.datetime(2022, 3, 14, tz='utc'),
    'owner': 'ale-filin'
}


with DAG('ale-filin-less4.2',
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['ale-filin']
) as dag:

    def get_week_day(str_date):
        return pendulum.from_format(str_date, 'YYYY-MM-DD').weekday() + 1

    def load_articles_from_gp_func(**context):
        week_day = get_week_day(context['ds'])
        get_article_sql = f"SELECT heading FROM articles WHERE id = {week_day}"

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(get_article_sql)
        query_res = cursor.fetchone()
        logging.info(query_res[0])



    load_articles_from_gp = PythonOperator(
        task_id='load_articles_from_gp',
        python_callable=load_articles_from_gp_func
    )

    end = DummyOperator(task_id="end")

    load_articles_from_gp >> end
