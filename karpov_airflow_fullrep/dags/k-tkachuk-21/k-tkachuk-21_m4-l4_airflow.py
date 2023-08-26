"""
m4-l4_airflow
homework
"""

import logging

from airflow import DAG
from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator


DEFAULT_ARGS = {
    'owner': 'k-tkachuk-21'
    , 'poke_interval': 600
    , 'start_date': datetime(2022, 3, 1)
    , 'end_date': datetime(2022, 3, 14)
}

with DAG(dag_id='k-tkachuk-21_m4-l4_airflow'
         , schedule_interval='0 7 * * 1-6'
         , default_args=DEFAULT_ARGS
         , max_active_runs=1
         , tags=['k-tkachuk-21']
         ) as dag:

    dummy = DummyOperator(task_id='dummy')

    def data_gp(execute):
        pg_hook = PostgresHook('conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        date_format = '%Y-%m-%d'
        date_execute = datetime.strptime(execute, date_format).date()
        date_of_week_execute = date_execute.isoweekday()
        query_sql = f"SELECT heading FROM public.articles WHERE id = {date_of_week_execute}"
        cursor.execute(query_sql)
        result = cursor.fetchall()
        logging.info(result)

        cursor.close()
        conn.close


    date_from_gp = PythonOperator(
        task_id='date_from_gp'
        , python_callable=data_gp
        , op_kwargs={'execute': '{{ ds }}'}
        , dag=dag
    )

    dummy >> date_from_gp
