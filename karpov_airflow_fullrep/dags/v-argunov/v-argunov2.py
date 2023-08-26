"""
First DAG v-argunov
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

import pendulum

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 4, 1, tz="UTC"),
    'end_date' : pendulum.datetime(2022, 4, 15, tz="UTC"),
    'owner': 'v-argunov',
    'poke_interval': 600
}
with DAG("v-argunov_lesson4",
    schedule_interval='0 0 * *  1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['karpov']
) as dag:

    def retrieve_from_greenplum_func(**kwargs):

        # Get day of the week
        week_num = pendulum.parse(kwargs['date']).day_of_week

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {week_num}')

        query_res = cursor.fetchall()
        logging.info(query_res)



    retrieve_from_greenplum = PythonOperator(
        task_id='retrieve_from_greenplum',
        python_callable=retrieve_from_greenplum_func,
        op_kwargs={'date': '{{ ds }}'}
    )

    retrieve_from_greenplum
