from airflow import DAG
from airflow.decorators import dag
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

import logging
from datetime import datetime


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'm-evgeniia-21'
}


@dag(
    dag_id='m-evgeniia-dag',
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=6,
    tags=['m-evgeniia-21']
)
def generate_dag():
    dummy = DummyOperator(task_id='dummy')

    ds_jm = BashOperator(
        task_id='ds_jm',
        bash_command='echo {{ ds }}'
    )

    def print_date_func(this_date):
        logging.info(f'ts_date: {this_date}')

    ts_jm = PythonOperator(
        task_id='ts_jm',
        python_callable=print_date_func,
        op_args=['{{ ts }}']
    )

    def get_heading(ds):
        heading_id = datetime.strptime(ds, '%Y-%m-%d').isoweekday()
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(heading_id))
        one_string = cursor.fetchone()[0]
        logging.info(one_string)

    print_heading_of_day = PythonOperator(
        task_id='print_heading_of_day',
        python_callable=get_heading,
        op_kwargs={'ds': '{{ ds }}'}
    )

    dummy >> ds_jm >> ts_jm >> print_heading_of_day


dag = generate_dag()
