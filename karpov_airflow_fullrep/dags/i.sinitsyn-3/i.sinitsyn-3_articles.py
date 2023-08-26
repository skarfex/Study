"""
lesson 4
"""
from airflow import DAG
import logging
from datetime import datetime, timedelta

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


DEFAULT_ARGS = {
    'owner': 'i.sinitsyn-3',
    'poke_interval': 300,
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14)
}

with DAG(
    dag_id='i.sinitsyn_4ls',
    schedule_interval='0 1 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['i.sinitsyn-3']
    ) as dag:

    dummy_start = DummyOperator(
        task_id='start_dag'
    )

    dummy_end = DummyOperator(
        task_id='end_dag'
    )
    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',  # выполняемый bash script (ds = execution_date)
        dag=dag
    )


    def get_data_greenplum(**kwargs):
        execution_dt = kwargs['ds']
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').isoweekday()
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {exec_day}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат


        logging.info('exec_day:{}'.format(exec_day))
        logging.info('query_res:{}'.format(query_res))


    print_sql_code = PythonOperator(
        task_id='print_sql_code',
        python_callable=get_data_greenplum,
        dag=dag
    )

    dummy_start >> echo_ds >> print_sql_code >> dummy_end
