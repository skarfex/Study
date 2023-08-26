"""
a.anisimov (дз по airflow - 3 и 4 уроки)
"""
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
import logging
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

DEFAULT_ARGS = {
    'owner': 'a.anisimov',
    'poke_interval': 600,
    'start_date': datetime(2022,3,1),
    'end_date': datetime(2022,3,14)
}


with DAG(
    dag_id='a.anisimov',
    schedule_interval='0 6 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a.anisimov'],
    catchup=True
) as dag:

    start_dummy_oper = DummyOperator(task_id="start_dummy_oper")
    
    echo_ds = BashOperator(task_id='echo_ds', bash_command='echo {{ ds }}', dag=dag)

    def str_shower(text='my_first_dag'):
        return logging.info(text)

    pyoper = PythonOperator(task_id='pyoper_str', python_callable=str_shower, dag=dag)

    def get_data_from_gp_func(exec_day):

        weekday = datetime.strptime(exec_day, '%Y-%m-%d').isoweekday()
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("get_heading_cursor")
        sql = f'SELECT heading FROM articles WHERE id = {weekday}'
        cursor.execute(sql)
        query_res = cursor.fetchall()

        logging.info('execution_date: {}'.format(exec_day))
        logging.info('day_num: {}'.format(weekday))
        logging.info('Article heading: {}'.format(query_res))

    get_heading = PythonOperator(
        task_id='get_heading',
        python_callable=get_data_from_gp_func,
        op_args=['{{ ds }}']
    )

    end_dummy_oper = DummyOperator(task_id='end_dummy_oper')

    start_dummy_oper >> echo_ds >> pyoper >> get_heading >> end_dummy_oper
