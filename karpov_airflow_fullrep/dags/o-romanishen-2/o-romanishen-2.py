"""
o-romanishen-2
dag для уроков 3 и 4 
"""
import datetime
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator


DEFAULT_ARGS = {
    'start_date': datetime.datetime(2022,3,1),
    'end_date': datetime.datetime(2022,3,14),
    'owner': 'o-romanishen-2',
    'retries': 3,
    'catchup': True
}

with DAG("o-romanishen_hw",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['lesson_3', 'lesson_4', 'lessons']
) as dag:

    weekday = DummyOperator(
        task_id = 'get_weekday'
    )


    def get_data_from_gp(**context):
        day_of_week = context['execution_date']
        day_of_week = day_of_week.weekday()+1

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn() 
        cursor = conn.cursor('o-romanishen-2')
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}') 
        response = cursor.fetchall()

        for row in response:
            logging.info(row)
    
    collect_result = PythonOperator(
        task_id = 'get_result',
        python_callable = get_data_from_gp

    )

weekday >> collect_result
