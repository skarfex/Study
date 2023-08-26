"""
Тестируем подключение в первом таске, а запрсы и зменения в поледующих
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging


from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-tagirov-15',
    'poke_interval':600
}


with DAG('atg_test_conn', # так наш даг будет называться в Airflow
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=["atg"]
         ) as dag:

    def conn_to_gp_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        logging.info('SUCCESS')
        return pg_hook

    conn_to_gp = PythonOperator(
        task_id=conn_to_gp,
        python_callable=conn_to_gp_func
    )

    def sql_request_func(pg_hook):
        conn = pg_hook.get_conn()
        cursor = conn.cursor('get_articles')
        sql_request = f'SELECT * FROM atg_cbr'
        cursor.execute(sql_request)
        query_res = cursor.fetchall()
        logging.info('????????????????????')
        logging.info(query_res)
        logging.info('????????????????????')

    sql_request = PythonOperator(
        task_id=sql_request,
        python_callable=sql_request_func
    )
conn_to_gp >> sql_request
