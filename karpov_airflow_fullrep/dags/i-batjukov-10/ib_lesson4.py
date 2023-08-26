"""
Igor Batyukov

Edited test DAG

"""
from airflow import DAG
from datetime import datetime
import logging


from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'i-batjukov-10',
    'poke_interval': 600
}

with DAG("ib_lesson4",
    schedule_interval='0 3 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['ib_lesson4']
) as dag:

    def load_from_gp_func(**kwargs):
        execution_dt = kwargs['templates_dict']['execution_dt']
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').isoweekday()
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT heading FROM articles WHERE id = %s', (exec_day,))
        query_res = cursor.fetchall()
        logging.info('-------------query result--------------')
        logging.info(f'execution_date: {execution_dt}')
        logging.info(f'day_num: {exec_day}')
        logging.info(f'Article heading: {query_res}')
        logging.info('---------------------------------------')

    load_from_gp = PythonOperator(
        task_id='load_from_gp',
        python_callable=load_from_gp_func,
        templates_dict={'execution_dt': '{{ ds }}'}
    )

load_from_gp
