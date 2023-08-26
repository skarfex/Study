from datetime import datetime

from airflow import DAG
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'n-zhujkov',
    'poke_interval': 600
}

with DAG("n-zhujkov_2nd_dag",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    catchup = True,
    tags=['n-zhujkov']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    def ext_from_gp_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute('SELECT heading FROM articles WHERE id = 1')
        return cursor.fetchall()

    ext_from_gp = PythonOperator(
        task_id='ext_from_gp',
        python_callable=ext_from_gp_func,
        dag=dag
    )

    def log_func(**kwargs):
        logging.info('--------------')
        logging.info(kwargs['templates_dict']['implicit'])
        logging.info('--------------')

    log = PythonOperator(
        task_id='log',
        python_callable=log_func,
        templates_dict={'implicit': '{{ ti.xcom_pull(task_ids="implicit_push") }}'},
        provide_context=True
    )

    dummy >> ext_from_gp >> log