"""
Даг для практики 2
"""
from datetime import datetime

from airflow import DAG
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'owner': 'i-huzin',
    'poke_interval': 600,
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14)
}

with DAG("i-huzin_practice2",
         schedule_interval='0 3 * * MON-SAT',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['i-huzin'],
         ) as dag:

    start = DummyOperator(task_id="start")

    def ft_load_from_gp(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # и именованный (необязательно) курсор
        weekday_number = kwargs['logical_date'].weekday()+1
        exec_date = kwargs['execution_date'].weekday() + 1
        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday_number}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат

        exec_ds = kwargs['templates_dict']['exec_ds']
        logging.info('-------------query result--------------')
        logging.info(f'exec_ds: {exec_ds}')
        logging.info(f'execution_date: {exec_date}')
        logging.info(f'execution_date jinja: {{execution_date}}')
        logging.info(f'logical_date: {weekday_number}')
        logging.info(f'query_res: {query_res}')
        logging.info('---------------------------------------')

    load_from_gp = PythonOperator(
        task_id='load_from_gp',
        python_callable=ft_load_from_gp,
        templates_dict={'exec_ds': '{{ ds }}'},
        provide_context=True
        #dag=dag
    )

    start >> load_from_gp
