"""
hard dag 2
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator

import logging
from datetime import datetime, time


DEFAULT_ARGS = {
    'owner': 'al-e',
    'email': 'pogromletov@gmail.com',
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False
}

with DAG("al-e_dag",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['al-e']
) as dag:

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='{{ ds }}',
        dag=dag
    )

    def get_row_from_gp(**kwargs):
        execution_dt = kwargs['ds']
        # exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday() + 1
        # pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        # conn = pg_hook.get_conn()
        # cursor = conn.cursor()
        # cursor.execute(f'SELECT heading FROM articles WHERE id = {exec_day}')  # исполняем sql
        # one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info(execution_dt)


    get_row = PythonOperator(
        task_id='get_row',
        python_callable=get_row_from_gp,
        dag=dag
    )

    echo_ds >> get_row