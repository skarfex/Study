"""
My Dag for load from  GreenPlum
"""
from airflow import DAG
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import datetime
from airflow.hooks.postgres_hook import PostgresHook
from dateutil import parser

DEFAULT_ARGS = {
    'owner': 's-jakovleva-20',
    'poke_interval': 600,
    'start_date': datetime.datetime(2022, 3, 1),
    'end_date': datetime.datetime(2022, 3, 14)
}

with DAG("ys_dag_GP",
    schedule_interval='0 3 * * 1,2,3,4,5,6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['s-jakovleva-20']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    def data_from_GP(execute):
        pg_hook = PostgresHook('conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        date_format = '%Y-%m-%d'
        date_execute = datetime.datetime.strptime(execute, date_format).date()
        date_of_week_execute = date_execute.weekday()
        query_sql = (f""" SELECT heading from public.articles where id = {int(date_of_week_execute)+1}""")
        cursor.execute(query_sql)
        result = cursor.fetchall()
        logging.info(result)

        cursor.close()
        conn.close


    date_from_gp = PythonOperator(
        task_id='date_from_gp',
        python_callable=data_from_GP,
        op_kwargs={'execute': '{{ ds }}'},
        dag=dag
    )

    dummy >> date_from_gp

