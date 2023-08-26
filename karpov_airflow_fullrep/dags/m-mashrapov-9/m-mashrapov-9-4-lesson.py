"""
Доработанный даг для задания (урок 4)
"""

from airflow import DAG
from datetime import datetime
import logging
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime (2022, 3, 1, 0, 0, 0),
    'end_date': datetime (2022, 3, 14, 23, 59, 59),
    'owner': 'm-mashrapov-9',
    'poke_interval': 600
}

dag = DAG("m-mashrapov-9",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=3,
          tags=['m-mashrapov-9']
)

dummy_op = DummyOperator(
    task_id='dummy_op',
    dag=dag
)

def select_day_func(**kwargs):
    execution_dt = kwargs['templates_dict']['execution_dt']
    exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
    return 'sunday' if exec_day == 6 else 'working_day'

week_day = BranchPythonOperator(
    task_id='week_day',
    python_callable=select_day_func,
    templates_dict={'execution_dt': '{{ ds }}'},
    dag=dag
)

def sunday_func():
    logging.info("Sunday")

sunday = PythonOperator(
    task_id='sunday',
    python_callable=sunday_func,
    dag=dag
)    

def get_heading_from_gp_func(heading_id):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor("named_cursor_name")
    cursor.execute(f"SELECT heading FROM articles WHERE id = {heading_id}")
    query_res = cursor.fetchall()
    logging.info(query_res)
    conn.close

working_day = PythonOperator(
    task_id='working_day',
    python_callable=get_heading_from_gp_func,
    op_args=['{{ execution_date.isoweekday() + 1}}'],
    dag=dag
) 

eod = DummyOperator(
    task_id='eod',
    trigger_rule='one_success',
    dag=dag
)

dummy_op >> week_day >> [sunday, working_day] >> eod
