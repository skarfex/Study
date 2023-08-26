import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import datetime
from airflow.hooks.postgres_hook import PostgresHook
import pendulum


DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1, tz="UTC"),
    'end_date': pendulum.datetime(2022, 3, 14, tz="UTC"),
    'owner': 'di-kudrjavtseva',
    'poke_interval': 600,
}


with DAG(
    dag_id='di-kudrjavtseva_hw_les_4',
    schedule_interval='20 16 * * 1-6',
    tags=['di-kudrjavtseva'],
    start_date=pendulum.datetime(2022, 3, 1, tz="UTC"),
    end_date=pendulum.datetime(2022, 3, 14, tz="UTC"),
) as dag:


    def get_day_of_week(day_of_week):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')
        query_res = cursor.fetchall()

        logging.info(query_res)

    get_day_of_week = PythonOperator(
                            task_id='get_day_of_week',
                            python_callable=get_day_of_week,
                            op_args=['{{ logical_date.weekday() + 1}}'])

    get_day_of_week