import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import datetime
from airflow.hooks.postgres_hook import PostgresHook
import pendulum


DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1, tz="UTC"),
    'end_date': pendulum.datetime(2022, 3, 14, tz="UTC"),
    'owner': 'g-tokarev-13',
    'poke_interval': 10,
}


with DAG(
    dag_id='g-tokarev-13_lesson_4',
    schedule_interval='0 0 * * 1-6',
    tags=['g-tokarev-13'],
    start_date=pendulum.datetime(2022, 3, 1, tz="UTC"),
    end_date=pendulum.datetime(2022, 3, 14, tz="UTC"),
) as dag:

    def get_day_of_week(**kwargs):
        day_of_week = datetime.strptime(kwargs['ds'], '%Y-%m-%d').weekday() + 1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')
        single_line = cursor.fetchone()[0]
        logging.info(single_line)
        kwargs['ti'].xcom_push(key='query_single_line', value=single_line)

    day_of_week_task = PythonOperator(
        task_id='day_of_week_task',
        python_callable=get_day_of_week,
    )