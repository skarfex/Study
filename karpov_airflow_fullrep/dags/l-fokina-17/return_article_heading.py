from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, time
from airflow.hooks.postgres_hook import PostgresHook
import logging

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'l-fokina-17',
    'poke_interval': 600
}

with DAG("return_article_heading",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['l-fokina']
         ) as dag:
    def get_string_for_today_func(**kwargs):
        this_date = kwargs['ds']
        day_of_week = datetime.strptime(this_date, '%Y-%m-%d').weekday() + 1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')
        one_string = cursor.fetchone()[0]
        logging.info('*****  DATABASE STRING ****')
        logging.info(one_string)
        logging.info('*********')
        logging.info(f'WEEKDAY: {day_of_week}')
        logging.info('*********')

    start_task = DummyOperator(task_id = "start_task")

    get_string_for_today = PythonOperator(
                               task_id='get_string_for_today',
                               python_callable=get_string_for_today_func,
                               provide_context = True
                               )

    end_task = DummyOperator(task_id = "end_task")

    start_task >> get_string_for_today >> end_task