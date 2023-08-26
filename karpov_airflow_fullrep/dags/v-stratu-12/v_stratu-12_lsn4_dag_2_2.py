"""
Доработанный DAG, который ходит в GreenPlum
c 1 по 14 марта 2022
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'depends_on_past': False,
    'owner': 'v-stratu-12',
    'poke_interval': 600
}

with DAG("v-stratu-12_lsn_4_dag_3",
          schedule_interval='0 0 * * 2-7',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['v-stratu-12']
          ) as dag:


    def go_to_gp_func(**kwargs):
        day_of_week = kwargs['execution_date'].weekday() + 1
        logging.info(f'Current day of week number is {day_of_week}')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'select heading from articles where id = {day_of_week}')
        kwargs['ti'].xcom_push(value=cursor.fetchone()[0], key='gp_data')


    def print_data_to_log_func(**kwargs):
        logging.info(kwargs['ti'].xcom_pull(task_ids='go_to_gp', key='gp_data'))


    go_to_gp = PythonOperator(
        task_id='go_to_gp',
        python_callable=go_to_gp_func,
        provide_context=True
    )

    print_data_to_log = PythonOperator(
        task_id='print_data_to_log',
        python_callable=print_data_to_log_func,
        provide_context=True
    )

go_to_gp >> print_data_to_log